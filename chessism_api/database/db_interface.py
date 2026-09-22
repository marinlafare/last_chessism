from typing import Any, List, Dict, TypeVar

from sqlalchemy import select, Integer, func, update, bindparam, case
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Base, Fen, to_dict, GameFenAssociation, Player, Month
from sqlalchemy.dialects.postgresql import insert as pg_insert

_ModelType = TypeVar("_ModelType", bound=Base)

DataObject = Dict[str, Any]
ListOfDataObjects = List[DataObject]

INSERT_BATCH_SIZE = 5000


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
        Inserts multiple records in a NEW session.
        Handles specific UPSERT logic for Fen.
        """
        async with AsyncDBSession() as session:
            try:
                # Use the new helper method, passing the session
                await self.create_all_with_session(session, data)
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                raise

    # --- NEW METHOD ---
    async def create_all_with_session(self, session: AsyncSession, data: ListOfDataObjects) -> bool:
        """
        Inserts multiple records using an EXISTING session.
        This is used for atomic transactions.
        """
        if not data:
            return True

        effective_batch_size = INSERT_BATCH_SIZE

        valid_columns = {c.name for c in self.db_class.__table__.columns}
        
        for start in range(0, len(data), effective_batch_size):
            chunk = data[start:start + effective_batch_size]
            if not chunk:
                continue
            
            clean_chunk = []
            for data_dict in chunk:
                clean_dict = {
                    k: v for k, v in data_dict.items() 
                    if k in valid_columns
                }
                clean_chunk.append(clean_dict)
            
            if not clean_chunk:
                continue 
            
            if self.db_class == Fen:
                # We define the statement *without* .values()
                stmt = pg_insert(self.db_class).on_conflict_do_update(
                    index_elements=[self.db_class.fen],
                    set_={
                        'n_games': (self.db_class.n_games.cast(Integer) + pg_insert(self.db_class).excluded.n_games.cast(Integer)),
                        'piece_count': func.coalesce(
                            self.db_class.piece_count,
                            pg_insert(self.db_class).excluded.piece_count,
                        ),
                        
                        # --- THIS IS THE SYNTAX FIX ---
                        'moves_counter': func.concat(
                            self.db_class.moves_counter,
                            case(
                                # (WHEN condition, THEN value)
                                (func.strpos(self.db_class.moves_counter, pg_insert(self.db_class).excluded.moves_counter) == 0,
                                 pg_insert(self.db_class).excluded.moves_counter),
                                # ELSE value
                                else_=''
                            )
                        )
                        # --- END SYNTAX FIX ---
                    }
                )
                # We pass the data as the *second argument*
                await session.execute(stmt, clean_chunk)
            
            elif self.db_class == GameFenAssociation:
                # We define the statement *without* .values()
                stmt = pg_insert(self.db_class).on_conflict_do_nothing(
                        index_elements=['game_link', 'fen_fen', 'n_move', 'move_color']
                )
                # We pass the data as the *second argument*
                await session.execute(stmt, clean_chunk)
            
            elif self.db_class == Player:
                # Player names are unique; ignore duplicates during bulk shell inserts.
                stmt = pg_insert(self.db_class).on_conflict_do_nothing(
                    index_elements=['player_name']
                )
                await session.execute(stmt, clean_chunk)

            elif self.db_class == Month:
                stmt = pg_insert(self.db_class).on_conflict_do_update(
                    index_elements=['player_name', 'year', 'month'],
                    set_={'n_games': pg_insert(self.db_class).excluded.n_games}
                )
                await session.execute(stmt, clean_chunk)
            
            else:
                await session.run_sync(
                    lambda sync_session, c=clean_chunk: sync_session.bulk_insert_mappings(self.db_class, c)
                )
        return True
    # --- END NEW METHOD ---


    async def update_fen_analysis_data(self,
                                            session: AsyncSession, # <-- MODIFIED: Use existing session
                                            analysis_data: ListOfDataObjects) -> int:
        """
        Updates 'score' and 'next_moves' for existing Fen records in a bulk operation
        using an EXISTING session (for transactional safety with FOR UPDATE).
        
        Args:
            session: The active AsyncSession holding the row locks.
            analysis_data: List of dicts, each with 'fen', 'score', 'next_moves'.
        
        Returns:
            The number of rows updated.
        """
        if not analysis_data:
            print("No analysis data provided for update.", flush=True)
            return 0
            
        # We do not use a 'with' block here, as the session is managed by the caller
        try:
            prepared_data = []
            for item in analysis_data:
                prepared_data.append({
                    'p_fen': item['fen'],
                    'p_piece_count': item.get('piece_count'),
                    'p_score': item['score'],
                    'p_next_moves': item['next_moves'],
                    'p_wdl_win': item.get('wdl_win'),
                    'p_wdl_draw': item.get('wdl_draw'),
                    'p_wdl_loss': item.get('wdl_loss')
                })

            # --- THE CORE FIX ---
            # Use self.db_class.__table__ (Core) instead of self.db_class (ORM)
            # to ensure compatibility with bulk parameter binding.
            stmt = (
                update(Fen.__table__) # <-- Use Fen table
                .where(Fen.fen == bindparam('p_fen'))
                .values(
                    piece_count=func.coalesce(
                        Fen.piece_count,
                        bindparam('p_piece_count'),
                    ),
                    score=bindparam('p_score'),
                    next_moves=bindparam('p_next_moves'),
                    wdl_win=bindparam('p_wdl_win'),
                    wdl_draw=bindparam('p_wdl_draw'),
                    wdl_loss=bindparam('p_wdl_loss'),
                    analysis_source='stockfish',
                    tablebase_wdl=None,
                    tablebase_dtz=None,
                    analyzed_at=func.now(),
                )
            )
            
            await session.execute(
                stmt,
                prepared_data,
                execution_options={"synchronize_session": False}
            )
            
            # --- COSMETIC FIX for -1 rowcount ---
            total_updated_rows = len(analysis_data)
            
            print(f"Successfully staged {total_updated_rows} FEN updates for analysis data (pending commit).", flush=True)
            
            # DO NOT COMMIT HERE. The caller (run_analysis_job) will commit.
            return total_updated_rows
        except Exception as e:
            # DO NOT ROLL BACK HERE. The caller will roll back.
            print(f"An error occurred during bulk update of FEN analysis data: {e}", flush=True)
            raise
