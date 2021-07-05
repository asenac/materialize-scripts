create materialized view metadataflow as
    with
         dataflow_channels as (select distinct c.id, o.address, source_node, source_port, target_node, target_port from mz_dataflow_channels c, mz_dataflow_operator_addresses o where o.id = c.id),
         dataflow_operators as (select distinct name, address, n.id from mz_dataflow_operators n, mz_dataflow_operator_addresses a where n.id = a.id)
    select c.address[1] as dataflow_id,
           c.id as channel_id, c.address as channel_address, source_node, source_port, target_node, target_port, sum(sent) as sent, sum(received) as received,
           (case when src.name is null then 'input_' || source_port else src.name end) as source_name, src.address as source_address, src.id as source_id,
           (case when dst.name is null then 'output_' || target_port else dst.name end) as target_name, dst.address as target_address, dst.id as target_id
    from
        dataflow_channels c
        left join mz_message_counts mc on c.id = mc.channel
        left join dataflow_operators src
            on c.address = src.address[1:list_length(src.address) - 1]
            and c.source_node = src.address[list_length(src.address)]
        left join dataflow_operators dst
            on c.address = dst.address[1:list_length(dst.address) - 1]
            and c.target_node = dst.address[list_length(dst.address)]
        group by c.id, c.address, source_node, source_port, target_node, target_port,
            src.name, src.address, src.id,
            dst.name, dst.address, dst.id;
