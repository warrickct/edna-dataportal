import logging
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, ForeignKey, String, Date, Float, Boolean
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
    # in context of edna, code represents the combined full name of the otu
    code = Column(String(length=1024))

    # we query OTUs via hierarchy, so indexes on the first few
    # layers are sufficient
    kingdom_id = ontology_fkey(OTUKingdom, index=True)
    phylum_id = ontology_fkey(OTUPhylum, index=True)
    class_id = ontology_fkey(OTUClass, index=True)
    order_id = ontology_fkey(OTUOrder)
    family_id = ontology_fkey(OTUFamily)
    genus_id = ontology_fkey(OTUGenus)
    species_id = ontology_fkey(OTUSpecies)
    amplicon_id = ontology_fkey(OTUAmplicon, index=True)
    endemic = Column(Boolean, default=True)

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

class SampleEnvironmentalMaterial1(OntologyMixin, Base):
    '''
    Tier 1 of environmental material classification
    '''
    pass

class SampleEnvironmentalMaterial2(OntologyMixin, Base):
    '''
    Tier 2 of environmental material classification
    '''
    pass
    
class SampleEnvironmentalMaterial3(OntologyMixin, Base):
    '''
    Tier 3 of environmental material classification
    '''
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
    // reverted: prepend underscore to var name to avoid keyword conflicts (OR) was causing issue.
    '''

    site = Column(CIText)

    x = Column(Float)
    y = Column(Float)
    road_len = Column(Float)
    area = Column(Float)
    rd_dens = Column(Float)
    prec_mean = Column(Float)
    prec_sd = Column(Float)
    prec_cv = Column(Float)
    ave_catchment_order = Column(Float)
    ave_river_protect = Column(Float)
    ave_imperviousness = Column(Float)
    ave_natural_cover = Column(Float)
    ave_pressure_sum = Column(Float)
    ave_lognconcen = Column(Float)
    mean_c_percent = Column(Float)
    mid_ph = Column(Float)
    masr = Column(Float)
    mat = Column(Float)
    elev = Column(Float)
    alp = Column(Float)
    bg = Column(Float)
    bgc = Column(Float)
    bgl = Column(Float)
    bl = Column(Float)
    brock = Column(Float)
    gy = Column(Float)
    hcpyb = Column(Float)
    hcyb = Column(Float)
    ice = Column(Float)
    irenyb = Column(Float)
    iybbl = Column(Float)
    iyblre = Column(Float)
    iyblyb = Column(Float)
    iybre = Column(Float)
    iygre = Column(Float)
    iygyb = Column(Float)
    lake = Column(Float)
    lit = Column(Float)
    msoil = Column(Float)
    oor = Column(Float)
    pod = Column(Float)
    pyb = Column(Float)
    pybl = Column(Float)
    pybp = Column(Float)
    pybp_or_ybl = Column(Float)
    r_or_yp_or_ybl = Column(Float)
    re = Column(Float)
    re_or_ybp = Column(Float)
    rend = Column(Float)
    rive = Column(Float)
    rl = Column(Float)
    sagy = Column(Float)
    saor = Column(Float)
    skele = Column(Float)
    town = Column(Float)
    uyb = Column(Float)
    uyg = Column(Float)
    yb = Column(Float)
    ybl = Column(Float)
    ybl_or_yb = Column(Float)
    ybp = Column(Float)
    ybp_or_ybl = Column(Float)
    ybs = Column(Float)
    ybst = Column(Float)
    yg = Column(Float)
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
    alpine_grass_or_herbfield = Column(Float)
    broadleaved_indigenous_hardwoods = Column(Float)
    built_dash_up_area_bracket_settlement_bracket_ = Column(Float)
    coastal_sand_and_gravel = Column(Float)
    deciduous_hardwoods = Column(Float)
    depleted_grassland = Column(Float)
    exotic_forest = Column(Float)
    fernland = Column(Float)
    flaxland = Column(Float)
    forest_dash_harvested = Column(Float)
    gorse_and_or_or_broom = Column(Float)
    gravel_and_rock = Column(Float)
    herbaceous_freshwater_vegetation = Column(Float)
    high_producing_exotic_grassland = Column(Float)
    indigenous_forest = Column(Float)
    lake_and_pond = Column(Float)
    landslide = Column(Float)
    low_producing_grassland = Column(Float)
    mangrove = Column(Float)
    manuka_and_or_or_kanuka = Column(Float)
    matagouri_or_grey_scrub = Column(Float)
    mixed_exotic_shrubland = Column(Float)
    orchard_vineyard_and_other_perennial_crops = Column(Float)
    permanent_snow_and_ice = Column(Float)
    river = Column(Float)
    short_dash_rotation_cropland = Column(Float)
    sub_alpine_shrubland = Column(Float)
    surface_mines_and_dumps = Column(Float)
    tall_tussock_grassland = Column(Float)
    transport_infrastructure = Column(Float)
    urban_parkland_or_open_space = Column(Float)
    richness = Column(Float)
    rapaport_node = Column(Float)

    # eDNA phase 3 fields
    # new meta fields

    region = Column(CIText, default = "unknown")
    vineyard = Column(CIText, default = "1")
    host_plant = Column(CIText, default = "unknown")

    # THIRD ITERATION NEW STRUCTURE
    project_number = Column(CIText, default="unknown")
    sample_identifier = Column(CIText, default="unknown")
    data_provider = Column(CIText, default="unknown")
    sequencing_platform = Column(CIText, default="unknown")
    amplicon = Column(CIText, default = "unknown")
    date_collected = Column(CIText, default = "01/02/2016")
    sequence_accession = Column(CIText, default="unknown")
    longitude = Column(Float)
    latitude = Column(Float)

    biome1 = Column(CIText, default = "terrestrial")
    biome2 = Column(CIText, default = "anthropogenic_terrestrial")
    biome3 = Column(CIText, default = "cropland")

    environmental_feature = Column(CIText, default = "organic")
    environmental_feature2 = Column(CIText, default = "organic")
    environmental_feature3 = Column(CIText, default = "organic")

    environmental_material1_id = ontology_fkey(SampleEnvironmentalMaterial1)
    environmental_material2_id = ontology_fkey(SampleEnvironmentalMaterial2)
    environmental_material3_id = ontology_fkey(SampleEnvironmentalMaterial3)
    elevation = Column(Float)
    rainfall = Column(Float)
    min_temp = Column(Float)
    max_temp = Column(Float)
    land_type = Column(CIText, default="unknown")
    soil_type = Column(CIText, default="unknown")
    conversation_status = Column(Boolean)
    regional_council = Column(CIText, default="unknown")
    iwi_area = Column(CIText, default="unknown")
    sample_description_area = Column(CIText, default="unknown")
    forward_primer = Column(CIText, default = "unknown")
    reverse_primer = Column(CIText, default = "unknown")

    # ONTOLOGICAL
    # TODO: implement same thing as sample_id except for tier 2 env feature
    environmental_material1 = relationship(SampleEnvironmentalMaterial1)
    environmental_material2 = relationship(SampleEnvironmentalMaterial2)

    sample_type_id = ontology_fkey(SampleType)
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

    proportional_abundance = Column(Float, nullable=False, default = 0)

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
    logger.info("engine string is: " + engine_string)
    return create_engine(engine_string)
