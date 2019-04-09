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
    endemic = Column(Boolean, default=False)
    pathogenic = Column(Boolean, default=False)

    kingdom = relationship(OTUKingdom)
    phylum = relationship(OTUPhylum)
    klass = relationship(OTUClass)
    order = relationship(OTUOrder)
    family = relationship(OTUFamily)
    genus = relationship(OTUGenus)
    species = relationship(OTUSpecies)

    def __repr__(self):
        return "<OTU(%d: %s,%s,%s,%s,%s,%s,%s,%s)>" % (
            self.id,
            # w: custom name field
            self.name,
            self.kingdom_id,
            self.phylum_id,
            self.class_id,
            self.order_id,
            self.family_id,
            self.genus_id,
            self.species_id
            )

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

class Biome_T1(OntologyMixin, Base):
    pass

class Biome_T2(OntologyMixin, Base):
    pass

class Biome_T3(OntologyMixin, Base):
    pass

class SampleContext(SchemaMixin, Base):
    '''
    Contextual table for sampling metadata
    '''
    __tablename__ = 'sample_context'

    # w: Making the row iteration the site id now.
    id = Column(Integer, primary_key=True)
    x = Column(Float, default=0)
    y = Column(Float, default=0)

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

    # date_collected = Column(Date, default = "unknown")
    date_collected = Column(CIText, default = "unknown")

    sequences_accession = Column(CIText, default="unknown")
    longitude = Column(Float)
    latitude = Column(Float)

    biome_t1 = Column(CIText, default = "unknown")
    biome_t2 = Column(CIText, default = "unknown")
    biome_t3 = Column(CIText, default = "unknown")

    environmental_feature_t1 = Column(CIText, default = "unknown")
    environmental_feature_t2 = Column(CIText, default = "unknown")
    environmental_feature_t3 = Column(CIText, default = "unknown")

    # environmental_material_t1= ontology_fkey(SampleEnvironmentalMaterial1)
    # environmental_material_t2= ontology_fkey(SampleEnvironmentalMaterial2)
    # environmental_material_t3= ontology_fkey(SampleEnvironmentalMaterial3)

    # TODO: Not sure if it's that efficient to ontologise the environments just yet since coloring is already working somewhat and sampleOtu results are searched by sample id anyway. Metafield filtering is still only compared to the <2000 site entries.

    environmental_material_t1= Column(CIText, default = "unknown")
    environmental_material_t2= Column(CIText, default = "unknown")
    environmental_material_t3= Column(CIText, default = "unknown")

    elevation = Column(Float, default=0)
    rainfall = Column(Float, default=0)
    min_temp = Column(Float, default=0)
    max_temp = Column(Float, default=0)
    land_type = Column(CIText, default="unknown")
    soil_type = Column(CIText, default="unknown")
    conservation_land_status = Column(Float, default=0)
    regional_council = Column(CIText, default="unknown")
    iwi_area = Column(CIText, default="unknown")
    sample_description = Column(CIText, default="unknown")
    primer_sequence_f = Column(CIText, default = "unknown")
    primer_sequence_r = Column(CIText, default = "unknown")

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

def make_engine():
    conf = settings.DATABASES['default']
    engine_string = 'postgres://%(USER)s:%(PASSWORD)s@%(HOST)s:%(PORT)s/%(NAME)s' % (conf)
    logger.info("engine string is: " + engine_string)
    return create_engine(engine_string)
