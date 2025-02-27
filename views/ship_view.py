import sqlite3
import json


def update_ship(id, ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Ship
                SET
                    name = ?,
                    hauler_id = ?
            WHERE id = ?
            """,
            (ship_data["name"], ship_data["hauler_id"], id),
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def delete_ship(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        DELETE FROM Ship WHERE id = ?
        """,
            (pk,),
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_ships(url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Default query gets basic ship data
        query = """
            SELECT
                s.id,
                s.name,
                s.hauler_id
            FROM Ship s
            """
        
        # Check if we need to expand hauler data
        expand_hauler = False
        if "query_params" in url and "_expand" in url["query_params"] and "hauler" in url["query_params"]["_expand"]:
            expand_hauler = True
            query = """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id,
                    h.id haulerId,
                    h.name haulerName,
                    h.dock_id
                FROM Ship s
                JOIN Hauler h
                    ON h.id = s.hauler_id
                """
        
        # Execute the query
        db_cursor.execute(query)
        
        # Fetch all rows
        query_results = db_cursor.fetchall()
        
        # Process the results
        ships = []
        for row in query_results:
            ship = dict(row)
            
            # If we expanded hauler data, format it properly
            if expand_hauler:
                hauler = {
                    "id": ship.pop("haulerId"),
                    "name": ship.pop("haulerName"),
                    "dock_id": ship.pop("dock_id")
                }
                ship["hauler"] = hauler
            
            ships.append(ship)
        
        # Serialize to JSON
        serialized_ships = json.dumps(ships)
        
        return serialized_ships


def retrieve_ship(pk, url):
    # Open a connection to an SQLite database file named "shipping.db"
    with sqlite3.connect("./shipping.db") as conn:
        # allows access the database results by column name 
        conn.row_factory = sqlite3.Row
        # creates a cursor object that allows you to execute SQL commands 
        db_cursor = conn.cursor()

        # Default query gets basic ship data
        # defines a basic SQL query that selects three columns (id, name, hauler_id) from the ship table 
        # the table is aliased as "s" for shorter references in the query 
        query = """
            SELECT
                s.id,
                s.name,
                s.hauler_id
            FROM Ship s
            WHERE s.id = ?
            """
        
        # Check if we need to expand hauler data
        # Checks if the URL contains a request to expand hauler data.
        # The URL object seems to have a nested dictionary structure with query parameters.
        # If the URL contains a parameter like _expand=hauler, then expand_hauler is set to True.
        if url.get("query_params") and url["query_params"].get("_expand") and "hauler" in url["query_params"]["_expand"]:

            # Joins the Ship table with the Hauler table
            # Selects additional hauler columns (id, name, dock_id).
            # Aliases the hauler columns to avoid name collisions with ship columns
            query = """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id,
                    h.id haulerId,
                    h.name haulerName,
                    h.dock_id
                FROM Ship s
                JOIN Hauler h
                    ON h.id = s.hauler_id
                WHERE s.id = ?
                """
        
        # Execute the query with the ship ID
        db_cursor.execute(query, (pk,))
        
        # Fetch the single row result
        row = db_cursor.fetchone()
        
        if not row:
            return None  # No ship found with that ID
        
        # Convert row to dictionary
        ship = dict(row)
        
        # If we expanded hauler data, format it properly
        # Creates a new dictionary for hauler data
        # Uses pop() to remove hauler fields from the ship dictionary 
        # Adds the hauler dictionary as a nested object under the key "hauler" in the ship
        if url.get("query_params") and url["query_params"].get("_expand") and "hauler" in url["query_params"]["_expand"]:
            hauler = {
                "id": ship.pop("haulerId"),
                "name": ship.pop("haulerName"),
                "dock_id": ship.pop("dock_id")
            }
            ship["hauler"] = hauler
        
        # Serialize to JSON
        serialized_ship = json.dumps(ship)
        
        return serialized_ship


def create_ship(ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
        INSERT INTO Ship
            (name, hauler_id)
        VALUES
            (?, ?)
        """,
            (ship_data["name"], ship_data["hauler_id"]),
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False
