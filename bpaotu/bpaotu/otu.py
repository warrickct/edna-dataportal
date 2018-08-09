import logging
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, ForeignKey, String, Date, Float
from django.conf import settings
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship
from citext import CIText


logger = logging.getLogger("rainbow")
Base = declarative_base()
SCHEMA = 'otu'


class SchemaMixin():
    """
    we use a specific schema (rather than the public schema) so that the import
    can be easily re-run, simply by deleting the schema. this also keeps
    SQLAlchemy tables out of the way of Django tables, and vice-versa
    """
    __table_args__ = {
        "schema": SCHEMA
    }


class OntologyMixin(SchemaMixin):
    id = Column(Integer, primary_key=True)
    value = Column(String, unique=True)

    @classmethod
    def make_tablename(cls, name):
        return 'ontology_' + name.lower()

    @declared_attr
    def __tablename__(cls):
        return cls.make_tablename(cls.__name__)

    def __repr__(self):
        return "<%s(%s)>" % (type(self).__name__, self.value)


def ontology_fkey(ontology_class, index=False):
    nm = ontology_class.__name__
    column = Column(Integer, ForeignKey(SCHEMA + '.' + OntologyMixin.make_tablename(nm) + '.id'), index=index)
    # stash this here for introspection later: saves a lot of manual
    # work with sqlalchemy's relationship() stuff
    column.ontology_class = ontology_class
    return column


def with_units(units, *args, **kwargs):
    column = Column(*args, **kwargs)
    column.units = units
    return column


class SampleType(OntologyMixin, Base):
    pass


class Environment(OntologyMixin, Base):
    pass


class OTUAmplicon(OntologyMixin, Base):
    pass


class OTUKingdom(OntologyMixin, Base):
    pass


class OTUPhylum(OntologyMixin, Base):
    pass


class OTUClass(OntologyMixin, Base):
    pass


class OTUOrder(OntologyMixin, Base):
    pass


class OTUFamily(OntologyMixin, Base):
    pass


class OTUGenus(OntologyMixin, Base):
    pass


class OTUSpecies(OntologyMixin, Base):
    pass


class OTU(SchemaMixin, Base):
    __tablename__ = 'otu'

    # w: commenting out the id being the pk for now.
    id = Column(Integer, primary_key=True)
    code = Column(String(length=1024))  # long GATTACAt-ype string

    # we query OTUs via heirarchy, so indexes on the first few
    # layers are sufficient
    kingdom_id = ontology_fkey(OTUKingdom, index=True)
    phylum_id = ontology_fkey(OTUPhylum, index=True)
    class_id = ontology_fkey(OTUClass, index=True)
    order_id = ontology_fkey(OTUOrder)
    family_id = ontology_fkey(OTUFamily)
    genus_id = ontology_fkey(OTUGenus)
    species_id = ontology_fkey(OTUSpecies)
    amplicon_id = ontology_fkey(OTUAmplicon, index=True)

    kingdom = relationship(OTUKingdom)
    phylum = relationship(OTUPhylum)
    klass = relationship(OTUClass)
    order = relationship(OTUOrder)
    family = relationship(OTUFamily)
    genus = relationship(OTUGenus)
    species = relationship(OTUSpecies)
    amplicon = relationship(OTUAmplicon)

    def __repr__(self):
        return "<OTU(%d: %s,%s,%s,%s,%s,%s,%s,%s)>" % (
            self.id,
            # w: custom name field
            self.name,
            self.amplicon_id,
            self.kingdom_id,
            self.phylum_id,
            self.class_id,
            self.order_id,
            self.family_id,
            self.genus_id,
            self.species_id
            )


class SampleHorizonClassification(OntologyMixin, Base):
    pass


class SampleStorageMethod(OntologyMixin, Base):
    pass


class SampleLandUse(OntologyMixin, Base):
    pass


class SampleEcologicalZone(OntologyMixin, Base):
    pass


class SampleVegetationType(OntologyMixin, Base):
    pass


class SampleProfilePosition(OntologyMixin, Base):
    pass


class SampleAustralianSoilClassification(OntologyMixin, Base):
    pass


class SampleFAOSoilClassification(OntologyMixin, Base):
    pass


class SampleTillage(OntologyMixin, Base):
    pass


class SampleColor(OntologyMixin, Base):
    pass

class SampleContext(SchemaMixin, Base):
    '''
    Site table 
    '''

    __tablename__ = 'sample_context'
    # id = Column(Integer, primary_key=True)  # NB: we use the final component of the ID here

    # # There are a large number of contextual fields, we are merging together all fields from BASE and MM
    # # so that they can be queried universally.
    # #
    # # Note that some columns are CIText when they would be better as either a Float or an ontology:
    # # as required we can work with the project managers to resolve data quality issues which force
    # # use to use a CIText column

    # w: Making the row iteration the site id now.
    id = Column(Integer, primary_key=True)

    # w: example  columns
    # a16s_comment = Column(CIText)
    # agrochemical_additions = Column(CIText)
    # allo = with_units('mg/m3', Float)
    # alpha_beta_car = with_units('mg/m3', Float)
    # ammonium = with_units('μmol/L', Float)
    # ammonium_nitrogen = with_units('mg/Kg', Float)


    # w: Cleaned up the columns using the regex patterns below.
    '''
    ' ' to _
    / to '_or_'
    & -> _and_
    '-' -> '_dash_'
    \(|\) -> '_bracket_'
    '__' -> '_' (from 1+ to 1.)
    Made all lowercase for consistency
    prepend underscore to var name to avoid keyword conflicts (OR) was causing issue.
    '''

    _site = Column(CIText)

    _x = Column(Float)
    _y = Column(Float)
    _road_len = Column(Float)
    _area = Column(Float)
    _rd_dens = Column(Float)
    _prec_mean = Column(Float)
    _prec_sd = Column(Float)
    _prec_cv = Column(Float)
    _ave_catchment_order = Column(Float)
    _ave_river_protect = Column(Float)
    _ave_imperviousness = Column(Float)
    _ave_natural_cover = Column(Float)
    _ave_pressure_sum = Column(Float)
    _ave_lognconcen = Column(Float)
    _mean_c_percent = Column(Float)
    _mid_ph = Column(Float)
    _masr = Column(Float)
    _mat = Column(Float)
    _elev = Column(Float)
    _alp = Column(Float)
    _bg = Column(Float)
    _bgc = Column(Float)
    _bgl = Column(Float)
    _bl = Column(Float)
    _brock = Column(Float)
    _gy = Column(Float)
    _hcpyb = Column(Float)
    _hcyb = Column(Float)
    _ice = Column(Float)
    _irenyb = Column(Float)
    _iybbl = Column(Float)
    _iyblre = Column(Float)
    _iyblyb = Column(Float)
    _iybre = Column(Float)
    _iygre = Column(Float)
    _iygyb = Column(Float)
    _lake = Column(Float)
    _lit = Column(Float)
    _msoil = Column(Float)
    _or = Column(Float)
    _pod = Column(Float)
    _pyb = Column(Float)
    _pybl = Column(Float)
    _pybp = Column(Float)
    _pybp_or_ybl = Column(Float)
    _r_or_yp_or_ybl = Column(Float)
    _re = Column(Float)
    _re_or_ybp = Column(Float)
    _rend = Column(Float)
    _rive = Column(Float)
    _rl = Column(Float)
    _sagy = Column(Float)
    _saor = Column(Float)
    _skele = Column(Float)
    _town = Column(Float)
    _uyb = Column(Float)
    _uyg = Column(Float)
    _yb = Column(Float)
    _ybl = Column(Float)
    _ybl_or_yb = Column(Float)
    _ybp = Column(Float)
    _ybp_or_ybl = Column(Float)
    _ybs = Column(Float)
    _ybst = Column(Float)
    _yg = Column(Float)
    _artificial2 = Column(Float)
    _bare2 = Column(Float)
    _water2 = Column(Float)
    _cropland2 = Column(Float)
    _grassland2 = Column(Float)
    _scrub2 = Column(Float)
    _forest2 = Column(Float)
    _artificial3 = Column(Float)
    _bare3 = Column(Float)
    _water3 = Column(Float)
    _cropland3 = Column(Float)
    _grassland3 = Column(Float)
    _scrub3 = Column(Float)
    _forest3 = Column(Float)
    _bare = Column(Float)
    _scrub = Column(Float)
    _exotic_grassland = Column(Float)
    _ind_forest = Column(Float)
    _tussock = Column(Float)
    _plantations = Column(Float)
    _urban = Column(Float)
    _freshwater = Column(Float)
    _marine = Column(Float)
    _high_alt = Column(Float)
    _low_alt = Column(Float)
    _urbanised = Column(Float)
    _aquatic_veg = Column(Float)
    _alpine_grass_or_herbfield = Column(Float)
    _broadleaved_indigenous_hardwoods = Column(Float)
    _built_dash_up_area_bracket_settlement_bracket_ = Column(Float)
    _coastal_sand_and_gravel = Column(Float)
    _deciduous_hardwoods = Column(Float)
    _depleted_grassland = Column(Float)
    _exotic_forest = Column(Float)
    _fernland = Column(Float)
    _flaxland = Column(Float)
    _forest_dash_harvested = Column(Float)
    _gorse_and_or_or_broom = Column(Float)
    _gravel_and_rock = Column(Float)
    _herbaceous_freshwater_vegetation = Column(Float)
    _high_producing_exotic_grassland = Column(Float)
    _indigenous_forest = Column(Float)
    _lake_and_pond = Column(Float)
    _landslide = Column(Float)
    _low_producing_grassland = Column(Float)
    _mangrove = Column(Float)
    _manuka_and_or_or_kanuka = Column(Float)
    _matagouri_or_grey_scrub = Column(Float)
    _mixed_exotic_shrubland = Column(Float)
    _orchard_vineyard_and_other_perennial_crops = Column(Float)
    _permanent_snow_and_ice = Column(Float)
    _river = Column(Float)
    _short_dash_rotation_cropland = Column(Float)
    _sub_alpine_shrubland = Column(Float)
    _surface_mines_and_dumps = Column(Float)
    _tall_tussock_grassland = Column(Float)
    _transport_infrastructure = Column(Float)
    _urban_parkland_or_open_space = Column(Float)
    _richness = Column(Float)
    _rapaport_node = Column(Float)

    # TEST: Made a sample type class which I'm guessing will become a field.
    _sample_type_id = ontology_fkey(SampleType)

    
    # w: Not using ontologies for now.
    #
    # ontologies
    #
    # australian_soil_classification_id = ontology_fkey(SampleAustralianSoilClassification)
    # broad_land_use_id = ontology_fkey(SampleLandUse)

    def __repr__(self):
        return "<SampleContext(%d)>" % (self.id)


class SampleOTU(SchemaMixin, Base):
    '''
    Combined table + site abundance values.
    '''
    __tablename__ = 'sample_otu'

    sample_id = Column(Integer, ForeignKey(SCHEMA + '.sample_context.id'), primary_key=True)
    otu_id = Column(Integer, ForeignKey(SCHEMA + '.otu.id'), primary_key=True)
    count = Column(Float, nullable=False)

    # TEMP: 
    def __repr__(self):
        return "<SampleOTU(%s,%s,%d)>" % (self.sample_id, self.otu_id, self.count)

    # ORIG:START: Original class contents. Rewrote the FK columns, the tostring and the PK.
    # sample_id = Column(Integer, ForeignKey(SCHEMA + '.sample_context.id'), primary_key=True)

    # otu_id = Column(Integer, ForeignKey(SCHEMA + '.otu.id'), primary_key=True)
    # count = Column(Integer, nullable=False)

    # def __repr__(self):
    #     return "<SampleOTU(%d,%d,%d)>" % (self.sample_id, self.otu_id, self.count)
    # ORIG:END:
    

def make_engine():
    conf = settings.DATABASES['default']
    engine_string = 'postgres://%(USER)s:%(PASSWORD)s@%(HOST)s:%(PORT)s/%(NAME)s' % (conf)
    return create_engine(engine_string)
