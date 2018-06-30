'''
    models.all - contains reference to all test models
'''
from .Person import Person
from .Meal import Meal

# ALL_MODELS - Order is important. During schema updates, tables will be created left-to-right
#    and drops will happen right-to-left.

# If the schema changes or a table is added or whatever, make sure to bump
ALL_MODELS = [Person, Meal]

__all__ = ('Person', 'Meal', 'ALL_MODELS')
