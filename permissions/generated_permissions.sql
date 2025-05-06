
GRANT USAGE ON SCHEMA ufdb TO dbexporter;

GRANT SELECT ON ufdb.t_analytical_journal_item TO dbexporter;

GRANT SELECT ON ufdb.t_delivery_order TO dbexporter;

GRANT SELECT ON ufdb.t_incoming_shipment TO dbexporter;

GRANT SELECT ON ufdb.t_instance TO dbexporter;

GRANT SELECT ON ufdb.t_internal_move TO dbexporter;

GRANT SELECT ON ufdb.t_internal_request TO dbexporter;

GRANT SELECT ON ufdb.t_inventory TO dbexporter;

GRANT SELECT ON ufdb.t_inventory_related_move TO dbexporter;

GRANT SELECT ON ufdb.t_location TO dbexporter;

GRANT SELECT ON ufdb.t_packing TO dbexporter;

GRANT SELECT ON ufdb.t_picking TO dbexporter;

GRANT SELECT ON ufdb.t_product TO dbexporter;

GRANT SELECT ON ufdb.t_product_category TO dbexporter;

GRANT SELECT ON ufdb.t_product_list TO dbexporter;

GRANT SELECT ON ufdb.t_product_price_history TO dbexporter;

GRANT SELECT ON ufdb.t_purchase_order TO dbexporter;

GRANT SELECT ON ufdb.t_sale_order TO dbexporter;

GRANT SELECT ON ufdb.t_shipment TO dbexporter;

GRANT SELECT ON ufdb.t_stock_move_inline TO dbexporter;

GRANT SELECT ON ufdb.t_supplier TO dbexporter;

GRANT SELECT ON ufdb.t_supplier_catalogue TO dbexporter;

GRANT SELECT ON ufdb.t_supplier_customer_document TO dbexporter;


GRANT SELECT on ufdb.t_journal_item TO dbexporter;
