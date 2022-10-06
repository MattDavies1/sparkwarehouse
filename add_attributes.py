# add attributes to pyspark.sql.Dataframe object
# Source: https://stackoverflow.com/questions/59651935/how-to-add-custom-method-to-pyspark-dataframe-class-by-inheritance

from pyspark.sql.dataframe import DataFrame
from functools import wraps

# decorator to attach a function to an attribute
def add_attr(cls):
    def decorator(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            f = func(*args, **kwargs)
            return f

        setattr(cls, func.__name__, _wrapper)
        return func

    return decorator

# custom functions
def custom(self):
    @add_attr(custom)
    def add_column3():
        return self.withColumn("col3", lit(3))

    @add_attr(custom)
    def add_column4():
        return self.withColumn("col4", lit(4))

    return custom

# add new property to the Class pyspark.sql.DataFrame
DataFrame.custom = property(custom)

# use it
df.custom.add_column3()

########## OR ##########

# Create a decorator to add a function to a python object
def add_attr(cls):
    def decorator(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            f = func(*args, **kwargs)
            return f

        setattr(cls, func.__name__, _wrapper)
        return func

    return decorator

  
# Extensions to the Spark DataFrame class go here
def dataframe_extension(self):
  @add_attr(dataframe_extension)
  def drop_fusion_gdpp_events():
    return(
      self
      .where(~((col('test1') == 'ABC') & (col('test2') =='XYZ')))
      .where(~col('test1').isin(['AAA', 'BBB']))
    )
  return dataframe_extension

DataFrame.dataframe_extension = property(dataframe_extension)

