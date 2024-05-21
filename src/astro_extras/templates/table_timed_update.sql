-- Close timed table records
update {{destination_table}} set effective_to = :effective_to where src_id = :src_id and effective_to is null