# data_lake_storage

storage_account_name = "stnlprodsharplive"

storage_account_key = "pQFYbOZNni2tQG73FpgpuVsW+ADU56DpZAynsZgrHsMo/ONoGHpDRli+C                                                                             xfwgAeGUpJr0/NklOXc+AStABUEVw=="

file_system_name = "fs-nl-prodsharp-live"  # default file_system in data lake

directory_name = "ufdb"  # default directory where to upload



# db_login

user = "dbexporter"

pwd = "djuGXr7OkqoCY4c"

database = "ufdb"

schema = "ufdb"



# misc

log_dir = "/home/dbexporter/dbexporter/logs"

output_dir = "/home/dbexporter/dbexporter/output"

num_workers = 4  # number of async workers

table_list = [
    't_analytic_account',

    't_analytic_journal_item',
    
    't_delivery_order',
    
    't_incoming_shipment',

    't_instance',

    't_internal_move',

    't_internal_request',
    
    't_inventory',
    
    't_inventory_related_move',
    
    't_journal_item',

    't_location',

    't_packing',

    't_picking',

    't_product',
    't_product_category',

    't_product_list',

    't_product_price_history',

    't_purchase_order',

    't_sale_order',

    't_shipment',

    't_stock_move_inline',

    't_supplier',

    't_supplier_catalogue',

    't_supplier_customer_document',
    
    't_unreserved_stock'

]



# acl

owner = '95df59a0-a760-4f92-93ac-b591badf537b'

group = 'a00abd7f-7600-42dc-9d65-a1462aafa4d5'

acl = 'user::rwx,group::rwx,other::---'

