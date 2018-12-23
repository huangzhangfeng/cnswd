from sqlalchemy.ext.declarative import declared_attr

from ..utils import to_table_name


class CommonMixin(object):
    @declared_attr
    def __tablename__(cls):
        name = cls.__name__
        return to_table_name(name)



