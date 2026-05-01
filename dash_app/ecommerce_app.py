import dash
from dash import html, dcc, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
from typing import Dict, List, Any, Optional
import json
from dataclasses import dataclass, asdict
from datetime import datetime
import uuid
import asyncio
import threading
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# Kafka Configuration (following project patterns)
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_BASKET_TOPIC = "basket-events-v2"
KAFKA_RECOMMENDATIONS_TOPIC = "recommendations-v2"
KAFKA_GROUP_ID = "ecommerce-dashboard-v2"

# Global state for recommendations
recommendations_data = {"items": []}
recommendations_lock = threading.Lock()

# Initialize the Dash app with Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "Fake E-Commerce Store"

@dataclass
class Product:
    """Product data class for type safety and clean code"""
    id: str
    name: str
    price: float
    description: str
    category: str
    image_url: str
    stock: int

@dataclass
class CartItem:
    """Cart item data class"""
    product_id: str
    quantity: int
    added_at: str

@dataclass
class BasketEvent:
    """Basket event data class for Kafka messages"""
    event_type: str  # "add", "remove", "update", "checkout"
    product_id: str
    product_name: str
    quantity: int
    price: float
    total_cart_value: float
    timestamp: str
    session_id: str
    cart_items: List[Dict[str, Any]]

class KafkaManager:
    """Manages Kafka producer and consumer operations"""
    
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.session_id = str(uuid.uuid4())
    
    async def start_producer(self):
        """Start Kafka producer"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        await self.producer.start()
        print(f"[Kafka] Producer started, bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    async def start_consumer(self):
        """Start Kafka consumer for recommendations"""
        self.consumer = AIOKafkaConsumer(
            KAFKA_RECOMMENDATIONS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: m.decode("utf-8"),
            auto_offset_reset="latest"
        )
        await self.consumer.start()
        print(f"[Kafka] Consumer started, topic: {KAFKA_RECOMMENDATIONS_TOPIC}")
    
    async def publish_basket_event(self, event: BasketEvent):
        """Publish basket event to Kafka"""
        if self.producer:
            try:
                message = asdict(event)
                await self.producer.send_and_wait(KAFKA_BASKET_TOPIC, message)
                print(f"[Kafka] Published basket event: {event.event_type} - {event.product_name}")
            except Exception as e:
                print(f"[Kafka] Error publishing basket event: {e}")
    
    async def consume_recommendations(self):
        """Consume recommendations from Kafka"""
        global recommendations_data
        if self.consumer:
            try:
                async for msg in self.consumer:
                    try:
                        data = json.loads(msg.value)
                        print(f"[Kafka] Received recommendation: {data}")
                        
                        with recommendations_lock:
                            # Update global recommendations state
                            if isinstance(data, dict) and "recommendations" in data:
                                recommendations_data = {
                                    "items": data["recommendations"],
                                    "timestamp": datetime.now().isoformat(),
                                    "session_id": data.get("session_id", "")
                                }
                                print(f"[Kafka] Updated recommendations_data: {len(data['recommendations'])} items")
                            else:
                                # Handle simple list format
                                recommendations_data = {
                                    "items": data if isinstance(data, list) else [data],
                                    "timestamp": datetime.now().isoformat()
                                }
                                print(f"[Kafka] Updated recommendations_data: simple format")
                    except json.JSONDecodeError as e:
                        print(f"[Kafka] Error parsing recommendation message: {e}")
            except Exception as e:
                print(f"[Kafka] Error consuming recommendations: {e}")
    
    async def stop(self):
        """Stop producer and consumer"""
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()

# Global Kafka manager
kafka_manager = KafkaManager()

class EcommerceStore:
    """Main store class to manage products and cart"""
    
    def __init__(self):
        self.products: Dict[str, Product] = {}
        self.cart: Dict[str, CartItem] = {}
        self._initialize_products()
    
    def _initialize_products(self) -> None:
        """Initialize the product catalog with fake data"""
        product_data = [
            {
                "id": "laptop-001",
                "name": "UltraBook Pro X1",
                "price": 1299.99,
                "description": "15-inch laptop with 16GB RAM, 512GB SSD, Intel i7 processor",
                "category": "Electronics",
                "image_url": "https://images.unsplash.com/photo-1496181133206-80ce9b88a853?w=300&h=200&fit=crop",
                "stock": 25
            },
            {
                "id": "phone-001", 
                "name": "SmartPhone Galaxy S23",
                "price": 899.99,
                "description": "6.1-inch OLED display, 128GB storage, 5G capable",
                "category": "Electronics",
                "image_url": "https://images.unsplash.com/photo-1511707171634-5f897ff02aa9?w=300&h=200&fit=crop",
                "stock": 50
            },
            {
                "id": "headphones-001",
                "name": "Wireless Noise Cancelling Headphones",
                "price": 299.99,
                "description": "Bluetooth 5.0, 30-hour battery life, premium sound quality",
                "category": "Electronics", 
                "image_url": "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?w=300&h=200&fit=crop",
                "stock": 30
            },
            {
                "id": "book-001",
                "name": "The Art of Programming",
                "price": 49.99,
                "description": "Comprehensive guide to modern software development",
                "category": "Books",
                "image_url": "https://images.unsplash.com/photo-1544947950-fa07a98d237f?w=300&h=200&fit=crop",
                "stock": 100
            },
            {
                "id": "coffee-001",
                "name": "Premium Coffee Beans",
                "price": 24.99,
                "description": "Single origin Arabica beans, medium roast, 1kg bag",
                "category": "Food & Beverages",
                "image_url": "https://images.unsplash.com/photo-1559056199-641a0ac8b55e?w=300&h=200&fit=crop",
                "stock": 75
            },
            {
                "id": "watch-001",
                "name": "Luxury Smart Watch",
                "price": 399.99,
                "description": "Heart rate monitor, GPS, water resistant, 7-day battery",
                "category": "Electronics",
                "image_url": "https://images.unsplash.com/photo-1523275335684-37898b6baf30?w=300&h=200&fit=crop",
                "stock": 15
            }
        ]
        
        for data in product_data:
            product = Product(**data)
            self.products[product.id] = product
    
    async def add_to_cart(self, product_id: str, quantity: int = 1) -> bool:
        """Add product to cart and publish to Kafka"""
        if product_id not in self.products:
            return False
        
        if product_id in self.cart:
            self.cart[product_id].quantity += quantity
        else:
            self.cart[product_id] = CartItem(
                product_id=product_id,
                quantity=quantity,
                added_at=datetime.now().isoformat()
            )
        
        # Publish basket event to Kafka
        product = self.products[product_id]
        event = BasketEvent(
            event_type="add",
            product_id=product_id,
            product_name=product.name,
            quantity=quantity,
            price=product.price,
            total_cart_value=self.get_cart_total(),
            timestamp=datetime.now().isoformat(),
            session_id=kafka_manager.session_id,
            cart_items=self.get_cart_items()
        )
        
        # Schedule the coroutine
        asyncio.create_task(kafka_manager.publish_basket_event(event))
        return True
    
    async def remove_from_cart(self, product_id: str) -> bool:
        """Remove product from cart and publish to Kafka"""
        if product_id in self.cart:
            product = self.products.get(product_id)
            if product:
                event = BasketEvent(
                    event_type="remove",
                    product_id=product_id,
                    product_name=product.name,
                    quantity=0,
                    price=product.price,
                    total_cart_value=self.get_cart_total(),
                    timestamp=datetime.now().isoformat(),
                    session_id=kafka_manager.session_id,
                    cart_items=self.get_cart_items()
                )
                asyncio.create_task(kafka_manager.publish_basket_event(event))
            
            del self.cart[product_id]
            return True
        return False
    
    async def update_cart_quantity(self, product_id: str, quantity: int) -> bool:
        """Update quantity of item in cart and publish to Kafka"""
        if product_id in self.cart:
            if quantity <= 0:
                return await self.remove_from_cart(product_id)
            else:
                old_quantity = self.cart[product_id].quantity
                self.cart[product_id].quantity = quantity
                
                # Publish update event
                product = self.products.get(product_id)
                if product:
                    event = BasketEvent(
                        event_type="update",
                        product_id=product_id,
                        product_name=product.name,
                        quantity=quantity,
                        price=product.price,
                        total_cart_value=self.get_cart_total(),
                        timestamp=datetime.now().isoformat(),
                        session_id=kafka_manager.session_id,
                        cart_items=self.get_cart_items()
                    )
                    asyncio.create_task(kafka_manager.publish_basket_event(event))
            return True
        return False
    
    def get_cart_total(self) -> float:
        """Calculate total cart value"""
        total = 0.0
        for item in self.cart.values():
            product = self.products.get(item.product_id)
            if product:
                total += product.price * item.quantity
        return total
    
    def get_cart_items(self) -> List[Dict[str, Any]]:
        """Get cart items with product details"""
        cart_items = []
        for item in self.cart.values():
            product = self.products.get(item.product_id)
            if product:
                cart_items.append({
                    "product_id": item.product_id,
                    "name": product.name,
                    "price": product.price,
                    "quantity": item.quantity,
                    "total": product.price * item.quantity,
                    "image_url": product.image_url
                })
        return cart_items
    
    def get_products_by_category(self, category: Optional[str] = None) -> List[Product]:
        """Get products filtered by category"""
        if category is None or category == "All":
            return list(self.products.values())
        return [p for p in self.products.values() if p.category == category]
    
    def get_categories(self) -> List[str]:
        """Get unique product categories"""
        return list(set(p.category for p in self.products.values()))
    
    async def checkout(self) -> bool:
        """Process checkout and publish event to Kafka"""
        if not self.cart:
            return False
        
        # Create checkout event
        total_value = self.get_cart_total()
        cart_items = self.get_cart_items()
        
        event = BasketEvent(
            event_type="checkout",
            product_id="multiple",
            product_name=f"Order with {len(cart_items)} items",
            quantity=sum(item["quantity"] for item in cart_items),
            price=total_value,
            total_cart_value=total_value,
            timestamp=datetime.now().isoformat(),
            session_id=kafka_manager.session_id,
            cart_items=cart_items
        )
        
        # Publish checkout event
        await kafka_manager.publish_basket_event(event)
        
        # Clear cart after checkout
        self.cart.clear()
        return True

# Initialize store
store = EcommerceStore()

def create_product_card(product: Product) -> dbc.Card:
    """Create a product card component"""
    return dbc.Card([
        dbc.CardImg(src=product.image_url, top=True, style={"height": "200px", "objectFit": "cover"}),
        dbc.CardBody([
            html.H5(product.name, className="card-title"),
            html.P(product.description, className="card-text", style={"fontSize": "0.9rem"}),
            html.H6(f"${product.price:.2f}", className="text-primary mb-3"),
            html.P(f"Stock: {product.stock}", className="text-muted small"),
            dbc.Button(
                "Add to Cart",
                id={"type": "add-to-cart", "index": product.id},
                color="primary",
                size="sm",
                className="w-100"
            )
        ])
    ], className="h-100")

def create_cart_item_card(item: Dict[str, Any]) -> dbc.Card:
    """Create a cart item card component"""
    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Img(
                        src=item["image_url"],
                        style={"width": "60px", "height": "60px", "objectFit": "cover"},
                        className="rounded"
                    )
                ], width=2),
                dbc.Col([
                    html.H6(item["name"], className="mb-1"),
                    html.P(f"${item['price']:.2f} each", className="text-muted small mb-2"),
                    dbc.InputGroup([
                        dbc.InputGroupText("Qty"),
                        dbc.Input(
                            type="number",
                            min=1,
                            value=item["quantity"],
                            id={"type": "cart-quantity", "index": item["product_id"]},
                            style={"maxWidth": "80px"}
                        ),
                        dbc.Button(
                            "Remove",
                            id={"type": "remove-from-cart", "index": item["product_id"]},
                            color="danger",
                            size="sm"
                        )
                    ], size="sm")
                ], width=10)
            ]),
            html.Hr(),
            html.H6(f"Total: ${item['total']:.2f}", className="text-end")
        ])
    ], className="mb-3")



# App layout
app.layout = dbc.Container([
    # Header
    dbc.Navbar(
        dbc.Container([
            dbc.NavbarBrand("🛒 Fake E-Commerce Store", className="fw-bold"),
            dbc.Nav([
                dbc.NavItem(dbc.NavLink("Products", href="#", id="nav-products", className="active")),
                dbc.NavItem(dbc.NavLink("Cart", href="#", id="nav-cart"))
            ], className="me-auto"),
            dbc.Badge(
                id="cart-badge",
                color="danger",
                className="fs-6"
            )
        ]),
        color="primary",
        dark=True,
        className="mb-4"
    ),
    
    # Recommendations panel (dedicated container)
    html.Div(id="recommendations-container"),
    
    # Main content area
    html.Div(id="main-content"),
    
    # Store cart data
    dcc.Store(id="cart-store", data={}),
    
    # Store recommendations data
    dcc.Store(id="recommendations-store", data={"items": [], "timestamp": ""}),
    
    # Notifications
    dbc.Toast(
        id="notification",
        header="Notification",
        is_open=False,
        dismissable=True,
        duration=3000,
        style={"position": "fixed", "top": "20px", "right": "20px", "zIndex": 1000}
    ),
    
    # Auto-refresh for recommendations
    dcc.Interval(id="recommendations-interval", interval=2000, n_intervals=0)
], fluid=True, className="py-4")

# Callback to poll and update recommendations store
@app.callback(
    Output("recommendations-store", "data"),
    [Input("recommendations-interval", "n_intervals")]
)
def update_recommendations_store(interval_n):
    """Poll global recommendations data and update the store"""
    with recommendations_lock:
        current_data = recommendations_data.copy()
    
    print(f"[UI Store] Polling recommendations: {len(current_data.get('items', []))} items")
    return current_data

# Callback to update recommendations panel based on store
@app.callback(
    Output("recommendations-container", "children"),
    [Input("recommendations-store", "data")]
)
def update_recommendations_panel(store_data):
    """Update recommendations panel when store data changes"""
    items = store_data.get("items", [])
    timestamp = store_data.get("timestamp", "")
    
    print(f"[UI Panel] Creating recommendations panel: {len(items)} items")
    
    if not items:
        return dbc.Card([
            dbc.CardHeader(html.H5("🤖 AI Recommendations", className="mb-0")),
            dbc.CardBody([
                html.P("No recommendations available yet. Add items to your cart to get personalized suggestions!", 
                       className="text-muted text-center")
            ])
        ], className="mb-4")
    
    recommendation_items = []
    for item in items[:3]:  # Show max 3 recommendations
        if isinstance(item, dict):
            product_name = item.get("name", item.get("product_name", "Unknown"))
            reason = item.get("reason", "Recommended for you")
            price = item.get("price", 0)
            
            recommendation_items.append(
                dbc.ListGroupItem([
                    html.H6(product_name, className="mb-1"),
                    html.P(reason, className="mb-1 text-muted small"),
                    html.P(f"${price:.2f}" if price > 0 else "Price TBD", className="text-success mb-0")
                ])
            )
        else:
            # Handle simple string recommendations
            recommendation_items.append(
                dbc.ListGroupItem([
                    html.H6(str(item), className="mb-0")
                ])
            )
    
    return dbc.Card([
        dbc.CardHeader([
            html.H5("🤖 AI Recommendations", className="mb-0"),
            dbc.Badge(f"{len(items)} items", color="success", className="ms-2")
        ]),
        dbc.CardBody([
            dbc.ListGroup(recommendation_items, flush=True),
            html.Small(f"Updated: {timestamp[:19] if timestamp else 'Never'}", 
                      className="text-muted")
        ])
    ], className="mb-4")

# Callback to update main content based on navigation
@app.callback(
    Output("main-content", "children"),
    [Input("nav-products", "n_clicks"),
     Input("nav-cart", "n_clicks")],
    prevent_initial_call=False
)
def update_main_content(products_clicks, cart_clicks):
    """Update main content based on navigation"""
    ctx = callback_context
    if not ctx.triggered:
        return create_products_page()
    
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    
    if button_id == "nav-cart":
        return create_cart_page()
    else:
        return create_products_page()

def create_products_page() -> html.Div:
    """Create the products page"""
    categories = ["All"] + store.get_categories()
    
    return html.Div([
        # Category filter
        dbc.Row([
            dbc.Col([
                html.H4("Our Products", className="mb-3"),
                dbc.Select(
                    id="category-filter",
                    options=[{"label": cat, "value": cat} for cat in categories],
                    value="All",
                    className="mb-4"
                )
            ])
        ]),
        
        # Products grid
        dbc.Row(id="products-grid")
    ])

def create_cart_page() -> html.Div:
    """Create the cart page"""
    cart_items = store.get_cart_items()
    total = store.get_cart_total()
    
    return html.Div([
        html.H4("Shopping Cart", className="mb-4"),
        
        # Cart items or empty message
        html.Div(id="cart-items-container"),
        
        # Cart summary (only show if items exist)
        html.Div(id="cart-summary-container")
    ])

# Callback to update products grid based on category filter
@app.callback(
    Output("products-grid", "children"),
    [Input("category-filter", "value")]
)
def update_products_grid(category):
    """Update products grid based on selected category"""
    products = store.get_products_by_category(category)
    
    if not products:
        return dbc.Alert("No products found in this category.", color="warning")
    
    # Create product cards in a responsive grid
    cards = []
    for product in products:
        cards.append(dbc.Col(create_product_card(product), className="mb-4", width=12, lg=4, md=6))
    
    return cards

# Callback to update cart items display
@app.callback(
    [Output("cart-items-container", "children"),
     Output("cart-summary-container", "children")],
    [Input("cart-store", "data")]
)
def update_cart_display(cart_data):
    """Update cart items and summary display"""
    cart_items = store.get_cart_items()
    total = store.get_cart_total()
    
    # Cart items
    if not cart_items:
        cart_content = dbc.Alert(
            "Your cart is empty. Add some products to get started!",
            color="info",
            className="text-center"
        )
        summary_content = html.Div()
    else:
        cart_content = [create_cart_item_card(item) for item in cart_items]
        
        # Cart summary
        summary_content = dbc.Card([
            dbc.CardBody([
                html.H5("Order Summary", className="card-title"),
                html.Hr(),
                html.Div([
                    html.H6(f"Total: ${total:.2f}", className="text-end mb-3"),
                    dbc.Button(
                        "Proceed to Checkout",
                        color="success",
                        size="lg",
                        className="w-100",
                        id="checkout-btn"
                    )
                ])
            ])
        ], className="mt-4")
    
    return cart_content, summary_content

# Callback to add product to cart
@app.callback(
    [Output("cart-store", "data"),
     Output("cart-badge", "children"),
     Output("notification", "is_open"),
     Output("notification", "children"),
     Output("notification", "header")],
    [Input({"type": "add-to-cart", "index": dash.ALL}, "n_clicks")],
    prevent_initial_call=True
)
def add_to_cart_callback(n_clicks):
    """Add product to cart"""
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    button_id = ctx.triggered[0]["prop_id"]
    product_id = json.loads(button_id.split(".")[0])["index"]
    
    # Create a new event loop for this callback if needed
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Run the async operation
    success = loop.run_until_complete(store.add_to_cart(product_id))
    
    if success:
        cart_items = store.get_cart_items()
        cart_count = sum(item["quantity"] for item in cart_items)
        
        product = store.products[product_id]
        notification_text = f"Added {product.name} to cart!"
        
        return cart_items, cart_count, True, notification_text, "Success"
    
    return dash.no_update, dash.no_update, True, "Failed to add item to cart", "Error"

# Callback to update cart quantity
@app.callback(
    [Output("cart-store", "data", allow_duplicate=True),
     Output("cart-badge", "children", allow_duplicate=True)],
    [Input({"type": "cart-quantity", "index": dash.ALL}, "value")],
    prevent_initial_call=True
)
def update_cart_quantity_callback(quantities):
    """Update cart item quantity"""
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update
    
    input_id = ctx.triggered[0]["prop_id"]
    product_id = json.loads(input_id.split(".")[0])["index"]
    
    # Find the corresponding quantity value
    new_quantity = None
    for qty in quantities:
        if qty is not None and qty > 0:
            new_quantity = qty
            break
    
    if new_quantity is not None:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        loop.run_until_complete(store.update_cart_quantity(product_id, new_quantity))
    
    cart_items = store.get_cart_items()
    cart_count = sum(item["quantity"] for item in cart_items)
    
    return cart_items, cart_count

# Callback to remove item from cart
@app.callback(
    [Output("cart-store", "data", allow_duplicate=True),
     Output("cart-badge", "children", allow_duplicate=True)],
    [Input({"type": "remove-from-cart", "index": dash.ALL}, "n_clicks")],
    prevent_initial_call=True
)
def remove_from_cart_callback(n_clicks):
    """Remove item from cart"""
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update
    
    button_id = ctx.triggered[0]["prop_id"]
    product_id = json.loads(button_id.split(".")[0])["index"]
    
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    loop.run_until_complete(store.remove_from_cart(product_id))
    
    cart_items = store.get_cart_items()
    cart_count = sum(item["quantity"] for item in cart_items)
    
    return cart_items, cart_count

# Callback to update cart badge on page load
@app.callback(
    Output("cart-badge", "children", allow_duplicate=True),
    [Input("main-content", "children")],
    prevent_initial_call=True
)
def update_cart_badge_on_load(content):
    """Update cart badge when page loads"""
    cart_items = store.get_cart_items()
    cart_count = sum(item["quantity"] for item in cart_items)
    return cart_count

# Callback to handle checkout
@app.callback(
    [Output("notification", "is_open", allow_duplicate=True),
     Output("notification", "children", allow_duplicate=True),
     Output("notification", "header", allow_duplicate=True),
     Output("cart-store", "data", allow_duplicate=True),
     Output("cart-badge", "children", allow_duplicate=True)],
    [Input("checkout-btn", "n_clicks")],
    prevent_initial_call=True
)
def handle_checkout_callback(n_clicks):
    """Handle checkout process"""
    if n_clicks:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Get total before checkout
        total = store.get_cart_total()
        
        # Process checkout
        success = loop.run_until_complete(store.checkout())
        
        if success:
            # Return updated state after checkout
            return (True, f"Order placed successfully! Total: ${total:.2f}", "Order Confirmed", 
                   [], 0)
        else:
            return (True, "Checkout failed - cart is empty", "Error", 
                   dash.no_update, dash.no_update)
    
    return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Kafka startup functions
def start_kafka_producer():
    """Start Kafka producer in a background thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def run_producer():
        try:
            await kafka_manager.start_producer()
            # Keep the producer alive
            while True:
                await asyncio.sleep(1)
        except Exception as e:
            print(f"[Kafka] Producer error: {e}")
        finally:
            await kafka_manager.stop()
    
    loop.run_until_complete(run_producer())

def start_kafka_consumer():
    """Start Kafka consumer in a background thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def run_consumer():
        try:
            await kafka_manager.start_consumer()
            await kafka_manager.consume_recommendations()
        except Exception as e:
            print(f"[Kafka] Consumer error: {e}")
        finally:
            await kafka_manager.stop()
    
    loop.run_until_complete(run_consumer())

if __name__ == "__main__":
    print("[*] Starting E-commerce application...")
    
    # Start Kafka producer in background thread
    producer_thread = threading.Thread(target=start_kafka_producer, daemon=True)
    producer_thread.start()
    
    # Start Kafka consumer in background thread
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()
    
    print("[*] Kafka producer and consumer started in background")
    print(f"[*] Session ID: {kafka_manager.session_id}")
    print(f"[*] Publishing basket events to: {KAFKA_BASKET_TOPIC}")
    print(f"[*] Listening for recommendations on: {KAFKA_RECOMMENDATIONS_TOPIC}")
    print("[*] Starting Dash app on http://127.0.0.1:8050")
    
    app.run(debug=True, host="0.0.0.0", port=8050)