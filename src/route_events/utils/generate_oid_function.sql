CREATE OR REPLACE FUNCTION generate_oid(
    oid_count in number,
    table_name in varchar,
    schema_name in varchar
)
    return sys.odcinumberlist pipelined as
    my_array sys.odcinumberlist;
BEGIN
    my_array := sys.odcinumberlist();
    my_array.extend(oid_count);
    
    -- Your PL/SQL code that populates the array
    for i in 1..oid_count loop
        my_array(i) := sde.gdb_util.next_rowid(schema_name, table_name);
    end loop;
    
    -- Convert array to table for selection
    FOR i IN 1..oid_count LOOP
        pipe row(my_array(i));
    END LOOP;
END;