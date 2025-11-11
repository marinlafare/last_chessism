#chessism_api/database/db_interface.py

import os
from typing import Any, List, Dict, TypeVar
from sqlalchemy import select, insert, Integer, func, update, bindparam # Import func
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.ext.asyncio import AsyncSession

# --- FIXED IMPORTS ---
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Base, Fen, to_dict, Game, game_fen_association
# ---

from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timezone

_ModelType = TypeVar("_ModelType", bound=Base)

DataObject = Dict[str, Any]
ListOfDataObjects = List[DataObject]

class DBInterface:
    def __init__(self, db_class: TypeVar('_ModelType', bound=Base)):
        self.db_class = db_class

    async def create(self, data: DataObject) -> DataObject:
        """
        Creates a single new record.
        """
        async with AsyncDBSession() as session:
            try:
                item: _ModelType = self.db_class(**data)
                session.add(item)
                await session.commit()
                await session.refresh(item)
                result = to_dict(item)
                return result
            except Exception as e:
                await session.rollback()
                raise

    async def read(self, **filters) -> ListOfDataObjects:
        """
        Reads records from the database based on filters.
        Returns a list of dictionaries.
        """
        async with AsyncDBSession() as session:
            try:
                stmt = select(self.db_class).filter_by(**filters)
                result = await session.execute(stmt)
                return [to_dict(row) for row in result.scalars().all()]
            except Exception as e:
                raise

    async def update(self, primary_key_value: Any, data: DataObject) -> DataObject | None:
        """
        Updates an existing record identified by its primary key.
        """
        async with AsyncDBSession() as session:
            try:
                item: _ModelType | None = await session.get(self.db_class, primary_key_value)
                if item is None:
                    return None
                for key, value in data.items():
                    if hasattr(item, key):
                        setattr(item, key, value)
                await session.commit()
                await session.refresh(item)
                return to_dict(item)
            except Exception as e:
                await session.rollback()
                raise

    async def delete(self, primary_key_value: Any) -> DataObject | None:
        """
        Deletes a record identified by its primary key.
        """
        async with AsyncDBSession() as session:
            try:
                item: _ModelType | None = await session.get(self.db_class, primary_key_value)
                if item is None:
                    return None
                result = to_dict(item)
                await session.delete(item)
                await session.commit()
                return result
            except Exception as e:
                await session.rollback()
                raise

    def get_session(self):
        """Returns an AsyncDBSession context manager."""
        return AsyncDBSession()

    async def create_all(self, data: ListOfDataObjects) -> bool:
        """
        Inserts multiple records. Handles specific UPSERT logic for Fen.
        For other models, it uses bulk_insert_mappings.
        **This method chunks inserts to avoid parameter limits.**
        """
        if not data:
            return True

        if self.db_class == Fen:
            params_per_row = 5 # 'fen', 'n_games', 'moves_counter', 'score', 'next_moves'
        else:
            params_per_row = len(self.db_class.__table__.columns) if hasattr(self.db_class, '__table__') else 3

        INSERT_BATCH_SIZE = 5000
        if params_per_row > 0:
            effective_batch_size = min(INSERT_BATCH_SIZE, 32000 // params_per_row)
            if effective_batch_size == 0:
                effective_batch_size = 1
        else:
            effective_batch_size = INSERT_BATCH_SIZE

        chunks = [data[i:i + effective_batch_size] for i in range(0, len(data), effective_batch_size)]

        async with AsyncDBSession() as session:
            try:
                for i, chunk in enumerate(chunks):
                    if not chunk:
                        continue
                    
                    if self.db_class == Fen:
                        stmt = pg_insert(self.db_class).values(chunk).on_conflict_do_update(
                            index_elements=[self.db_class.fen],
                            set_={
                                'n_games': (self.db_class.n_games.cast(Integer) + pg_insert(self.db_class).excluded.n_games.cast(Integer)),
                                'moves_counter': pg_insert(self.db_class).excluded.moves_counter,
                                'next_moves': None,
                                'score': None
                            }
                        )
                        await session.execute(stmt) # Add await here
                    else:
                        await session.run_sync(
                            lambda sync_session, c=chunk: sync_session.bulk_insert_mappings(self.db_class, c)
                        )
                        # Commit after each non-Fen chunk, as bulk_insert_mappings
                        # doesn't auto-join the async transaction context
                        await session.commit() 
                        continue # Skip the final commit for this chunk

                # Final commit for all pg_insert (Fen) chunks
                if self.db_class == Fen:
                    await session.commit()
                
                return True
            except Exception as e:
                await session.rollback()
                raise

    async def upsert_main_fens(self,
                                 objects_to_insert: ListOfDataObjects,
                                 objects_to_update: ListOfDataObjects) -> bool:
        """
        Inserts new MainFen records and updates existing ones.
        ---
        PERFORMANCE WARNING: The update logic (for item in objects_to_update)
        runs session.get() inside a loop. This is an N+1 query problem
        and will be very slow for large update lists.
        ---
        """
        if not objects_to_insert and not objects_to_update:
            return True

        async with AsyncDBSession() as session:
            try:
                # --- Process Inserts ---
                if objects_to_insert:
                    insert_stmt = pg_insert(Fen).values(objects_to_insert).on_conflict_do_nothing(
                        index_elements=[Fen.fen]
                    )
                    await session.execute(insert_stmt)

                # --- Process Updates (N+1 Query Problem) ---
                if objects_to_update:
                    for item_data in objects_to_update:
                        fen_to_update = item_data['fen']
                        new_moves_counter = item_data['moves_counter']
                        
                        # This session.get() is inside a loop, causing N+1 queries.
                        db_item = await session.get(Fen, fen_to_update)
                        
                        if db_item:
                            existing_moves_counter = db_item.moves_counter
                            if new_moves_counter not in existing_moves_counter:
                                updated_moves_counter = existing_moves_counter + new_moves_counter
                            else:
                                updated_moves_counter = existing_moves_counter
                                
                            db_item.n_games += item_data['n_games']
                            db_item.moves_counter = updated_moves_counter
                            db_item.next_moves = item_data['next_moves']
                            db_item.score = item_data['score']

                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                raise

    async def associate_fen_with_games(self, associations_to_insert_raw: List[Dict[str, Any]]) -> bool:
        """
        Associates multiple FENs with their respective lists of games in a bulk operation
        by directly inserting into the association table.

        Args:
            associations_to_insert_raw: A list of dictionaries, e.g.:
                [{"game_link": 123, "fen_fen": "r1bqk..."},
                 {"game_link": 124, "fen_fen": "r1b1k..."}]
        """
        if not associations_to_insert_raw:
            print("No valid associations to insert after processing input data.")
            return True

        params_per_row = 2
        INSERT_BATCH_SIZE = 5000
        effective_batch_size = min(INSERT_BATCH_SIZE, 32000 // params_per_row)
        if effective_batch_size == 0:
            effective_batch_size = 1

        chunks = [associations_to_insert_raw[i:i + effective_batch_size]
                  for i in range(0, len(associations_to_insert_raw), effective_batch_size)]

        async with AsyncDBSession() as session:
            try:
                total_inserted_rows = 0
                for i, chunk in enumerate(chunks):
                    if not chunk:
                        continue

                    insert_stmt = pg_insert(game_fen_association).values(chunk).on_conflict_do_nothing(
                        index_elements=[game_fen_association.c.game_link, game_fen_association.c.fen_fen]
                    )
                    result = await session.execute(insert_stmt)
                    total_inserted_rows += result.rowcount

                await session.commit()
                print(f"Successfully committed a total of {total_inserted_rows} new associations.")
                return True

            except Exception as e:
                await session.rollback()
                print(f"An error occurred during bulk FEN-Game association: {e}")
                raise

    async def update_all(self, data: ListOfDataObjects) -> bool:
        """
        Updates multiple Game records to set 'fens_done' = True.
        
        Args:
            data: A list of game links (Integers) to be updated.
        """
        if not data:
            print(f"No data provided for bulk update of {self.db_class.__tablename__}.")
            return True

        primary_key_column = Game.link
        links_to_update = data
        BATCH_SIZE = 10000

        chunks = [links_to_update[i:i + BATCH_SIZE] for i in range(0, len(links_to_update), BATCH_SIZE)]

        async with AsyncDBSession() as session:
            try:
                total_updated_rows = 0
                for i, chunk in enumerate(chunks):
                    if not chunk:
                        continue

                    stmt = (
                        update(Game)
                        .where(primary_key_column.in_(chunk))
                        .values(fens_done=True)
                    )
                    
                    result = await session.execute(stmt)
                    total_updated_rows += result.rowcount

                await session.commit()
                print(f"Successfully committed a total of {total_updated_rows} game updates for 'fens_done'.")
                return True

            except Exception as e:
                await session.rollback()
                print(f"An error occurred during bulk update of game 'fens_done': {e}")
                raise
                
    async def update_fen_analysis_data(self, analysis_data: ListOfDataObjects) -> int:
        """
        Updates 'score' and 'next_moves' for existing Fen records in a bulk operation.
        
        Args:
            analysis_data: List of dicts, each with 'fen', 'score', 'next_moves'.
        
        Returns:
            The number of rows updated.
        """
        if not analysis_data:
            print("No analysis data provided for update.")
            return 0

        async with AsyncDBSession() as session:
            try:
                # Prepare data for bulk update.
                # Keys must match the bindparam names.
                prepared_data = []
                for item in analysis_data:
                    prepared_data.append({
                        'p_fen': item['fen'],
                        'p_score': item['score'],
                        'p_next_moves': item['next_moves']
                    })

                stmt = (
                    update(self.db_class)
                    .where(self.db_class.fen == bindparam('p_fen'))
                    .values(
                        score=bindparam('p_score'),
                        next_moves=bindparam('p_next_moves')
                    )
                )
                
                # --- PERFORMANCE FIX ---
                # Execute the bulk update in a single call, not in a loop.
                result = await session.execute(
                    stmt,
                    prepared_data, # Pass the entire list of parameters
                    execution_options={"synchronize_session": False}
                )
                total_updated_rows = result.rowcount
                # --- END FIX ---

                await session.commit()
                print(f"Successfully updated {total_updated_rows} FEN records with analysis data.")
                return total_updated_rows
            except Exception as e:
                await session.rollback()
                print(f"An error occurred during bulk update of FEN analysis data: {e}")
                raise
                
async def reset_all_game_fens_done_to_false() -> int:
    """
    Resets the 'fens_done' column to False for all Game records where it is currently True.
    """
    async with AsyncDBSession() as session:
        try:
            stmt = (
                update(Game)
                .where(Game.fens_done == True)
                .values(fens_done=False)
            )
            result = await session.execute(stmt)
            await session.commit()
            print(f"Successfully reset 'fens_done' to False for {result.rowcount} game(s).")
            return result.rowcount
        except Exception as e:
            await session.rollback()
            print(f"An error occurred while resetting 'fens_done' status: {e}")
            raise