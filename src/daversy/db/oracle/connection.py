DEFAULT_NLS_LANG = 'AMERICAN_AMERICA.AL32UTF8'

try:
    import os
    os.environ['NLS_LANG'] = DEFAULT_NLS_LANG
    import cx_Oracle
except ImportError:
    OracleConnection = None
else:
    class OracleConnection(object):
        def __init__(self, info):
            self.connection = cx_Oracle.connect(info[0])
            self.setup_dbms_metadata()
            
        def cursor(self):
            return self.connection.cursor()
            
        def setup_dbms_metadata(self):
            cursor = self.connection.cursor()
            cursor.execute("""
                DECLARE
                  ncount INTEGER;
                BEGIN
                    dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR', true);
                    SELECT count(*)    INTO ncount 
                    FROM  user_objects WHERE status <> 'VALID';
                    IF ncount > 0 THEN
                       dbms_utility.compile_schema(USER);
                    END IF;
                END;""")
            cursor.close()

        def close(self):
            self.connection.close()
