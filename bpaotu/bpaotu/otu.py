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
    # id = Column(Integer, primary_key=True)

    # w: replacement id column without being pk
    id = Column(Integer)

    # w: will just steal this already made code field to store the taxonomy name
    code = Column(String(length=1024), primary_key=True)  # long GATTACAt-ype string

    # we query OTUs via heirarchy, so indexes on the first few
    # layers are sufficient

    # w: not using these for now.
    # kingdom_id = ontology_fkey(OTUKingdom, index=True)
    # phylum_id = ontology_fkey(OTUPhylum, index=True)
    # class_id = ontology_fkey(OTUClass, index=True)
    # order_id = ontology_fkey(OTUOrder)
    # family_id = ontology_fkey(OTUFamily)
    # genus_id = ontology_fkey(OTUGenus)
    # species_id = ontology_fkey(OTUSpecies)
    # amplicon_id = ontology_fkey(OTUAmplicon, index=True)

    # kingdom = relationship(OTUKingdom)
    # phylum = relationship(OTUPhylum)
    # klass = relationship(OTUClass)
    # order = relationship(OTUOrder)
    # family = relationship(OTUFamily)
    # genus = relationship(OTUGenus)
    # species = relationship(OTUSpecies)
    # amplicon = relationship(OTUAmplicon)

    def __repr__(self):
        return "<OTU(%d: %s,%s,%s,%s,%s,%s,%s,%s)>" % (
            self.id,
            # w: custom name field
            # self.name,
            # w: just having the pk and full id for now.
            # self.amplicon_id,
            # self.kingdom_id,
            # self.phylum_id,
            # self.class_id,
            # self.order_id,
            # self.family_id,
            # self.genus_id,
            # self.species_id
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

    # w: primary key example = "ABK"
    id = Column(String, primary_key=True)

    # w: trying to keep meta fields simple for now.
    x = with_units('lng', Float)
    y = with_units('lat', Float)

    # w: example  columns
    # a16s_comment = Column(CIText)
    # agrochemical_additions = Column(CIText)
    # allo = with_units('mg/m3', Float)
    # alpha_beta_car = with_units('mg/m3', Float)
    # ammonium = with_units('μmol/L', Float)
    # ammonium_nitrogen = with_units('mg/Kg', Float)

    #replacements:
    '''
    ' ' to _
    / to '_or_'
    & -> _and_
    '-' -> '_dash_'
    \(|\) -> '_bracket_'
    '__' -> '_' (from 1+ to 1.)
    NOTE: Keeping the casing for now because one of the sites uses the word 'or' which is a python keyword.
    '''

    # w:TODO: REGEX THIS
	site = Column(Float)
	x = Column(Float)
	y = Column(Float)
	road_len = Column(Float)
	area = Column(Float)
	rd_dens = Column(Float)
	prec_mean = Column(Float)
	prec_sd = Column(Float)
	prec_cv = Column(Float)
	Ave_catchment_order = Column(Float)
	Ave_river_protect = Column(Float)
	Ave_imperviousness = Column(Float)
	Ave_natural_cover = Column(Float)
	Ave_pressure_sum = Column(Float)
	ave_logNconcen = Column(Float)
	mean_C_percent = Column(Float)
	mid_pH = Column(Float)
	masr = Column(Float)
	mat = Column(Float)
	elev = Column(Float)
	ALP = Column(Float)
	BG = Column(Float)
	BGC = Column(Float)
	BGL = Column(Float)
	BL = Column(Float)
	BRock = Column(Float)
	GY = Column(Float)
	HCPYB = Column(Float)
	HCYB = Column(Float)
	ice = Column(Float)
	IRENYB = Column(Float)
	IYBBL = Column(Float)
	IYBLRE = Column(Float)
	IYBLYB = Column(Float)
	IYBRE = Column(Float)
	IYGRE = Column(Float)
	IYGYB = Column(Float)
	lake = Column(Float)
	LIT = Column(Float)
	MSoil = Column(Float)
	OR = Column(Float)
	POD = Column(Float)
	PYB = Column(Float)
	PYBL = Column(Float)
	PYBP = Column(Float)
	PYBP_or_YBL = Column(Float)
	R_or_YP_or_YBL = Column(Float)
	RE = Column(Float)
	RE_or_YBP = Column(Float)
	REND = Column(Float)
	rive = Column(Float)
	RL = Column(Float)
	SAGY = Column(Float)
	SAOR = Column(Float)
	SKele = Column(Float)
	town = Column(Float)
	UYB = Column(Float)
	UYG = Column(Float)
	YB = Column(Float)
	YBL = Column(Float)
	YBL_or_YB = Column(Float)
	YBP = Column(Float)
	YBP_or_YBL = Column(Float)
	YBS = Column(Float)
	YBST = Column(Float)
	YG = Column(Float)
	artificial2 = Column(Float)
	bare2 = Column(Float)
	water2 = Column(Float)
	cropland2 = Column(Float)
	grassland2 = Column(Float)
	scrub2 = Column(Float)
	forest2 = Column(Float)
	artificial3 = Column(Float)
	bare3 = Column(Float)
	water3 = Column(Float)
	cropland3 = Column(Float)
	grassland3 = Column(Float)
	scrub3 = Column(Float)
	forest3 = Column(Float)
	bare = Column(Float)
	scrub = Column(Float)
	exotic_grassland = Column(Float)
	ind_forest = Column(Float)
	tussock = Column(Float)
	plantations = Column(Float)
	urban = Column(Float)
	freshwater = Column(Float)
	marine = Column(Float)
	high_alt = Column(Float)
	low_alt = Column(Float)
	urbanised = Column(Float)
	aquatic_veg = Column(Float)
	Alpine_Grass_or_Herbfield = Column(Float)
	Broadleaved_Indigenous_Hardwoods = Column(Float)
	Built_dash_up_Area_bracket_settlement_bracket_ = Column(Float)
	Coastal_Sand_and_Gravel = Column(Float)
	Deciduous_Hardwoods = Column(Float)
	Depleted_Grassland = Column(Float)
	Exotic_Forest = Column(Float)
	Fernland = Column(Float)
	Flaxland = Column(Float)
	Forest_dash_Harvested = Column(Float)
	Gorse_and_or_or_Broom = Column(Float)
	Gravel_and_Rock = Column(Float)
	Herbaceous_Freshwater_Vegetation = Column(Float)
	High_Producing_Exotic_Grassland = Column(Float)
	Indigenous_Forest = Column(Float)
	Lake_and_Pond = Column(Float)
	Landslide = Column(Float)
	Low_Producing_Grassland = Column(Float)
	Mangrove = Column(Float)
	Manuka_and_or_or_Kanuka = Column(Float)
	Matagouri_or_Grey_Scrub = Column(Float)
	Mixed_Exotic_Shrubland = Column(Float)
	Orchard_Vineyard_and_Other_Perennial_Crops = Column(Float)
	Permanent_Snow_and_Ice = Column(Float)
	River = Column(Float)
	Short_dash_rotation_Cropland = Column(Float)
	Sub_Alpine_Shrubland = Column(Float)
	Surface_Mines_and_Dumps = Column(Float)
	Tall_Tussock_Grassland = Column(Float)
	Transport_Infrastructure = Column(Float)
	Urban_Parkland_or_Open_Space = Column(Float)
	Richness = Column(Float)
	Rapaport_node = Column(Float)

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
    sample_id = Column(String, ForeignKey(SCHEMA + '.sample_context.id'), primary_key=True)

    # otu_id = Column(Integer, ForeignKey(SCHEMA + '.otu.id'), primary_key=True)
    # w: TEST: testing if I can jsut use the bacteria name as the FK for now.
    otu_id = Column(String, ForeignKey(SCHEMA + '.otu.code'), primary_key=True)

    # w: custom column dealing with float abundance instead of int count.
    count = Column(Float, nullable=False)

    # count = Column(Integer, nullable=False)

    # w: think I have to make this represent as strings too
    # def __repr__(self):
    #     return "<SampleOTU(%d,%d,%d)>" % (self.sample_id, self.otu_id, self.count)

    # TEMP: 
    def __repr__(self):
        return "<SampleOTU(%s,%s,%d)>" % (self.sample_id, self.otu_id, self.count)


def make_engine():
    conf = settings.DATABASES['default']
    engine_string = 'postgres://%(USER)s:%(PASSWORD)s@%(HOST)s:%(PORT)s/%(NAME)s' % (conf)
    return create_engine(engine_string)
