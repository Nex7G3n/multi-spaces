CLIENTES_FIELDS = {
    "cliente_id": "int", 
    "nombre": "str", 
    "email": "str", 
    "telefono": "str", 
    "direccion": "str"
}

PERSONAL_FIELDS = {
    "personal_id": "int", 
    "nombre": "str", 
    "rol": "str"
}

PRODUCTO_FIELDS = {
    "producto_id": "int", 
    "nombre": "str", 
    "precio": "decimal", 
    "stock": "int"
}

FACTURA_FIELDS = {
    "factura_id": "int", 
    "cliente_id": "int", 
    "personal_id": "int", 
    "fecha": "datetime", 
    "total": "decimal"
}

DETALLE_FACTURA_FIELDS = {
    "detalle_id": "int", 
    "factura_id": "int", 
    "producto_id": "int", 
    "cantidad": "int", 
    "precio_unitario": "decimal", 
    "subtotal": "decimal"
}

TABLE_DEFINITIONS = {
    "Clientes": {"pk": "cliente_id", "fields": CLIENTES_FIELDS},
    "Personal": {"pk": "personal_id", "fields": PERSONAL_FIELDS},
    "Producto": {"pk": "producto_id", "fields": PRODUCTO_FIELDS},
    "Factura": {"pk": "factura_id", "fields": FACTURA_FIELDS},
    "Detalle_Factura": {"pk": "detalle_id", "fields": DETALLE_FACTURA_FIELDS} # Corregido el nombre de la tabla
}
