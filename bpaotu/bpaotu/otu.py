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

    count = Column(Integer, nullable=False)

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
