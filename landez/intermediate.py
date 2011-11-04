from abc import ABCMeta, abstractmethod
import hashlib
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, Integer, LargeBinary, MetaData
from sqlalchemy.orm import sessionmaker


class IntermediateStorage(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_tile_filename(self, filename, x, y, z):
        pass

    # @abstractmethod
    # def get_tile_data(self, x, y, z):
    #     pass

    @abstractmethod
    def all_tiles(self):
        pass

    def cleanup(self):
        pass


Base = declarative_base()


class SQLStorage(IntermediateStorage):

    def __init__(self, db_url):
        IntermediateStorage.__init__(self)
        self.engine = create_engine(db_url)
        self.make_session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def add_tile_handle(self, handle, x, y, z):
        return self.add_tile(handle.read(), x, y, z)

    def add_tile_filename(self, filename, x, y, z):
        with open(filename) as f_in:
            return self.add_tile_handle(f_in, x, y, z)
        
    def add_tile(self, file_data, x, y, z):
        session = self.make_session()
        data_hash = hashlib.md5(file_data).hexdigest()
        if not session.query(TileData).get(data_hash):
            td = TileData()
            td.hash = data_hash
            td.data = file_data
            session.add(td)
        tile = Tile()
        tile.x = x
        tile.y = y
        tile.zoom = z
        tile.hash = data_hash
        session.add(tile)
        session.commit()
        session.close()

    def all_tiles(self):
        offset = 0
        limit = 100
        session = self.make_session()
        tiles = None
        tiles = session.query(Tile).offset(offset).limit(limit)
        while tiles.count() > 0:
            for tile in tiles:
                tile_data = session.query(TileData).get(tile.hash)
                yield (tile.x, tile.y, tile.zoom, tile_data.data)
            offset += limit
            tiles = session.query(Tile).offset(offset).limit(limit)

    def cleanup(self):
        meta = MetaData(self.engine)
        meta.drop_all()


IntermediateStorage.register(SQLStorage)


class Tile(Base):
    __tablename__ = 'tile'
    id = Column(Integer(), primary_key=True)
    hash = Column(String(32))
    x = Column(Integer())
    y = Column(Integer())
    zoom = Column(Integer())

class TileData(Base):
    __tablename__ = 'tile_data'
    hash = Column(String(32), primary_key=True)
    data = Column(LargeBinary())

