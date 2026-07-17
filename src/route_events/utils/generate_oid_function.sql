create or replace FUNCTION generate_oid(
    oid_count in number,
    table_name  in varchar,
    schema_name in varchar
)
    return sys.odcinumberlist pipelined
as
BEGIN
    FOR i IN 1..oid_count LOOP
        pipe row( sde.gdb_util.next_rowid(schema_name, table_name) );
    END LOOP;
END;