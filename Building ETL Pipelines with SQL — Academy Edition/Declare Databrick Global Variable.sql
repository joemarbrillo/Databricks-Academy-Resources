-- Create the global SQL variable with the catalog name using the temp view
DECLARE OR REPLACE my_catalog STRING;
SET VAR my_catalog = (SELECT catalog_name FROM catalog_name_vw);


-- Set default catalog to the user's course catalog
USE CATALOG IDENTIFIER(my_catalog);