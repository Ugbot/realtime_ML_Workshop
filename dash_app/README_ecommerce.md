# Fake E-Commerce Store

A modern, responsive e-commerce website built with Dash and Bootstrap. This application demonstrates a complete shopping experience with product catalog, shopping cart, and checkout functionality.

## Features

- 🛍️ **Product Catalog**: Browse products with images, descriptions, and pricing
- 🏷️ **Category Filtering**: Filter products by category (Electronics, Books, Food & Beverages)
- 🛒 **Shopping Cart**: Add/remove items, update quantities, view totals
- 📱 **Responsive Design**: Works on desktop, tablet, and mobile devices
- 🔔 **Notifications**: Real-time feedback for user actions
- 💳 **Checkout Process**: Complete order placement workflow

## Product Categories

- **Electronics**: Laptops, smartphones, headphones, smartwatches
- **Books**: Programming and technical books
- **Food & Beverages**: Premium coffee and beverages

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Run the test script to verify functionality:
```bash
python test_ecommerce.py
```

3. Start the application:
```bash
python ecommerce_app.py
```

4. Open your browser and navigate to: `http://localhost:8050`

## Usage

### Browsing Products
- Navigate to the "Products" tab to view all available items
- Use the category dropdown to filter products by category
- Click "Add to Cart" to add items to your shopping cart

### Managing Cart
- Click the "Cart" tab to view your shopping cart
- Update quantities using the number input fields
- Click "Remove" to remove items from cart
- View the cart total and order summary

### Checkout
- Review your cart items and total
- Click "Proceed to Checkout" to complete your order
- Receive confirmation notification

## Technical Details

### Architecture
- **Object-Oriented Design**: Clean separation of concerns with `EcommerceStore`, `Product`, and `CartItem` classes
- **Type Annotations**: Full type safety with Python type hints
- **Dataclasses**: Modern Python dataclasses for data structures
- **Bootstrap Integration**: Professional UI with responsive components

### Key Components

#### EcommerceStore Class
- Manages product catalog and shopping cart
- Handles cart operations (add, remove, update quantities)
- Calculates totals and provides filtered product views

#### Product Data Structure
```python
@dataclass
class Product:
    id: str
    name: str
    price: float
    description: str
    category: str
    image_url: str
    stock: int
```

#### Cart Management
- Persistent cart state during session
- Real-time quantity updates
- Automatic total calculations

### UI Components
- **Navigation Bar**: Clean header with cart badge
- **Product Cards**: Responsive grid layout with product images
- **Cart Interface**: Detailed cart view with quantity controls
- **Notifications**: Toast notifications for user feedback

## Testing

Run the integration test to verify all functionality:
```bash
python test_ecommerce.py
```

The test covers:
- Product loading and display
- Category filtering
- Cart operations (add, remove, update)
- Total calculations
- Error handling

## Customization

### Adding New Products
Edit the `_initialize_products()` method in the `EcommerceStore` class:

```python
{
    "id": "unique-product-id",
    "name": "Product Name",
    "price": 99.99,
    "description": "Product description",
    "category": "Category Name",
    "image_url": "https://example.com/image.jpg",
    "stock": 50
}
```

### Styling
- Uses Bootstrap 5 for responsive design
- Custom CSS can be added to `app.css` for additional styling
- Color scheme and components can be customized via Bootstrap classes

## Dependencies

- `dash==2.17.1`: Main web framework
- `dash-bootstrap-components==1.5.0`: Bootstrap UI components
- `plotly==5.17.0`: Interactive charts and graphs
- `pandas==2.1.4`: Data manipulation (if needed for future features)

## Browser Compatibility

- Chrome/Chromium (recommended)
- Firefox
- Safari
- Edge

## Future Enhancements

- User authentication and accounts
- Order history and tracking
- Product reviews and ratings
- Payment processing integration
- Inventory management
- Admin dashboard
- Search functionality
- Wishlist feature 