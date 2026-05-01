#!/usr/bin/env python3
"""
Test script for the e-commerce store functionality
"""

import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ecommerce_app import EcommerceStore, Product, CartItem

async def test_store_functionality():
    """Test the e-commerce store functionality"""
    print("🧪 Testing E-commerce Store Functionality")
    print("=" * 50)
    
    # Initialize store
    store = EcommerceStore()
    
    # Test 1: Check if products are loaded
    print(f"✅ Products loaded: {len(store.products)}")
    for product in store.products.values():
        print(f"   - {product.name}: ${product.price:.2f}")
    
    # Test 2: Test category filtering
    categories = store.get_categories()
    print(f"\n✅ Categories found: {categories}")
    
    electronics = store.get_products_by_category("Electronics")
    print(f"   - Electronics products: {len(electronics)}")
    
    # Test 3: Test cart operations
    print("\n🛒 Testing Cart Operations:")
    
    # Add items to cart (async)
    laptop_id = "laptop-001"
    phone_id = "phone-001"
    
    await store.add_to_cart(laptop_id, 2)
    await store.add_to_cart(phone_id, 1)
    
    cart_items = store.get_cart_items()
    print(f"   - Items in cart: {len(cart_items)}")
    for item in cart_items:
        print(f"     * {item['name']}: {item['quantity']} x ${item['price']:.2f} = ${item['total']:.2f}")
    
    # Test cart total
    total = store.get_cart_total()
    print(f"   - Cart total: ${total:.2f}")
    
    # Test quantity update (async)
    await store.update_cart_quantity(laptop_id, 3)
    updated_total = store.get_cart_total()
    print(f"   - Updated cart total: ${updated_total:.2f}")
    
    # Test remove item (async)
    await store.remove_from_cart(phone_id)
    final_items = store.get_cart_items()
    print(f"   - Items after removal: {len(final_items)}")
    
    final_total = store.get_cart_total()
    print(f"   - Final cart total: ${final_total:.2f}")
    
    # Test checkout (async)
    await store.checkout()
    checkout_items = store.get_cart_items()
    checkout_total = store.get_cart_total()
    print(f"   - Items after checkout: {len(checkout_items)}")
    print(f"   - Total after checkout: ${checkout_total:.2f}")
    
    print("\n✅ All tests passed!")
    return True

if __name__ == "__main__":
    try:
        asyncio.run(test_store_functionality())
        print("\n🎉 E-commerce store is working correctly!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 