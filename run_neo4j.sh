# Tweak memory settings to allow loading large graphs
docker run \
    -p 7474:7474 -p 7687:7687 \
    -v $PWD/neo4j/data:/data -v $PWD/neo4j/plugins:/plugins \
    -v $PWD/graphs:/var/lib/neo4j/import \
    --name neo4j-apoc \
    -e NEO4J_apoc_export_file_enabled=true \
    -e NEO4J_apoc_import_file_enabled=true \
    -e NEO4J_apoc_import_file_use__neo4j__config=true \
    -e NEO4J_PLUGINS=\[\"apoc\"\] \
    -e NEO4J_AUTH=none \
    -e NEO4J_server_memory_pagecache_size=1G \
    -e NEO4J_server_memory_heap_initial__size=8G \
    -e NEO4J_server_memory_heap_max__size=8G \
    neo4j