"""Independent database adapters: PostgreSQL is authoritative; Neo4j is derived.

Import the required backend explicitly. Importing this package never connects
to either service, initializes schemas, or triggers synchronization.
"""
