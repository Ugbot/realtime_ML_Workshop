import logging
import sys
import os

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.table import StreamTableEnvironment

# SpeechRecognition library is a third-party dependency.
# Ensure it's installed in your Flink environment.
# You might also need to install an audio processing library like pydub
# and an offline speech recognition engine like CMU Sphinx (PocketSphinx).
# pip install SpeechRecognition pydub pocketsphinx
import speech_recognition as sr
from pydub import AudioSegment

# Configuration (replace with your actual values)
KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
KAFKA_TOPIC_INPUT: str = "wave_file_uris"
KAFKA_CONSUMER_GROUP_ID: str = "flink_speech_to_text_group"

class SpeechToTextMapFunction(MapFunction):
    """
    UDF to perform speech-to-text on a local WAVE file URI.
    """

    def __init__(self):
        self.recognizer = None

    def open(self, runtime_context: RuntimeContext):
        self.recognizer = sr.Recognizer()

    def map(self, file_uri: str) -> tuple[str, str]:
        """
        Takes a file URI, loads the WAVE file, and transcribes it.
        Returns a tuple of (file_uri, transcribed_text or error_message).
        """
        try:
            # Assuming the URI is for a local file.
            # If it's a URI like 'file:///path/to/audio.wav', strip 'file://'
            if file_uri.startswith("file://"):
                file_path = file_uri[7:]
            else:
                file_path = file_uri

            if not os.path.exists(file_path):
                logging.warning(f"File not found: {file_path}")
                return file_uri, f"ERROR: File not found at {file_path}"

            # Load audio file (Pydub can handle various formats including WAV)
            # And convert to a format SpeechRecognition can use (AudioData instance)
            # AudioSegment.from_wav expects a .wav file.
            # If other formats might come, more robust loading is needed.
            logging.info(f"Processing file: {file_path}")
            audio = AudioSegment.from_wav(file_path)
            
            # SpeechRecognition library expects audio data in a specific format.
            # We'll export the audio in WAV format (if it wasn't already)
            # and then use sr.AudioFile.
            # Alternatively, pass raw data if possible/more efficient.
            # For simplicity, using sr.AudioFile which handles reading from disk.
            with sr.AudioFile(file_path) as source:
                audio_data = self.recognizer.record(source) # read the entire audio file

            # Recognize speech using Sphinx (offline)
            # This requires CMU Sphinx to be installed and configured.
            # For other engines (e.g., Google Web Speech API), change the method
            # and handle API keys/authentication.
            text: str = self.recognizer.recognize_sphinx(audio_data)
            logging.info(f"Successfully transcribed {file_uri}: {text}")
            return file_uri, text

        except sr.UnknownValueError:
            logging.warning(f"Sphinx could not understand audio from {file_uri}")
            return file_uri, "ERROR: Sphinx could not understand audio"
        except sr.RequestError as e:
            logging.error(f"Sphinx error with {file_uri}; {e}")
            return file_uri, f"ERROR: Sphinx error; {e}"
        except Exception as e:
            logging.error(f"Error processing file {file_uri}: {e}", exc_info=True)
            return file_uri, f"ERROR: {str(e)}"

def run_speech_to_text_job():
    """
    Main function to define and execute the PyFlink job.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # Optional: Set parallelism
    # env.set_parallelism(1)

    # Table API environment (can be useful for some connector features or SQL)
    # t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # Define the Kafka source
    # Assuming the Kafka messages are simple strings (the file URI)
    # If they are JSON, adjust the deserialization schema
    # For simplicity, let's assume plain string URIs for now.
    # If it's JSON like {"uri": "file:///path/to/file.wav"},
    # a JsonRowDeserializationSchema is more appropriate.

    # For plain string messages:
    # SimpleStringSchema is not directly available in the new Kafka connector setup.
    # We can use JsonRowDeserializationSchema if messages are JSON objects like {"uri": "..."}.
    # For now, let's assume a JSON structure with a "uri" field.
    uri_type_info = Types.ROW([Types.STRING()]) # For a single field named "uri"
    
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(uri_type_info) \
        .build()

    kafka_source = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC_INPUT,
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_CONSUMER_GROUP_ID,
            # 'auto.offset.reset': 'earliest' # Start from the earliest offset if no committed offset
        }
    )
    # The Kafka consumer expects messages like: {"uri_field_name": "file:///path/to/audio.wav"}
    # We then need to extract this field. Let's assume the field is named "uri".
    
    # Add Kafka source to the environment
    # data_stream = env.add_source(kafka_source).name("KafkaWaveFileURISource")
    # The output type of JsonRowDeserializationSchema is Row. We need to extract the string.
    # data_stream_uris = data_stream.map(lambda row: row[0], output_type=Types.STRING())


    # If Kafka messages are plain strings (not JSON)
    # A common pattern is to use a custom DeserializationSchema or a simple map after a byte-based schema.
    # For now, using a placeholder for direct string deserialization for simplicity
    # (Actual implementation might require a custom deserializer or ensuring Kafka message is formatted as JSON)
    # This part needs refinement based on actual Kafka message format.
    # A common simple approach if messages are plain strings:
    # 1. Read as bytes.
    # 2. Map bytes to string.
    
    # Let's adjust to expect a simple string URI directly for now,
    # and handle the deserialization more simply. The official Kafka connector
    # is evolving. For this example, we'll assume a simpler way to get strings.
    # A typical way is to get byte[] and then decode.
    # However, the PyFlink Kafka connector examples usually show a schema.
    # Let's assume the Kafka topic sends JSON messages like `{"uri": "file_path.wav"}`

    json_deserializer = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW([Types.STRING()])) \
        .build() # Expects {"f0": "uri_value"} if not specifying field names mapping.
                 # Or more robustly: .type_info(Types.ROW_NAMED(["uri"], [Types.STRING()]))

    # Let's use a named field for clarity in the JSON.
    deserialization_schema_named = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW_NAMED(["uri"], [Types.STRING()])) \
        .build()

    kafka_source_named = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC_INPUT,
        deserialization_schema=deserialization_schema_named,
        properties={
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest' 
        }
    )
    # Kafka messages should be: {"uri": "file:///path/to/my_audio.wav"}
    
    data_stream_with_row = env.add_source(kafka_source_named).name("KafkaWaveFileURISource")

    # Extract the URI string from the Row object
    uri_stream = data_stream_with_row.map(lambda row: row.uri, output_type=Types.STRING()) \
        .name("ExtractURIString")

    # Apply the speech-to-text UDF
    # The output of SpeechToTextMapFunction is Tuple[str, str]
    output_type_info = Types.TUPLE([Types.STRING(), Types.STRING()])
    transcribed_stream = uri_stream.map(SpeechToTextMapFunction(), output_type=output_type_info) \
        .name("SpeechToText")

    # Print the results to standard output
    transcribed_stream.print().name("PrintToStdout")

    # Execute the Flink job
    job_name = "PyFlink Speech to Text from Kafka"
    logging.info(f"Starting Flink job: {job_name}")
    env.execute(job_name)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run_speech_to_text_job()
    except Exception as e:
        logging.error("Job failed", exc_info=True) 