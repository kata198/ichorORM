'''    
    Copyright (c) 2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE

    relations - Defines relation types (One-to-one, one-to-many)
'''


__all__ = ( 'ForeignRelation', 'isForeignRelationType', 'RelationIntegrityError', 'OneToOneRelation', 'OneToManyRelation',  )

class ForeignRelation(object):
    '''
        ForeignRelation - Base class of foreign relations
    '''

def isForeignRelationType(obj):
    '''
        isForeignRelationType - Check if passed object extends ForeignRelation

            @return <bool> - True if #obj extends ForeignRelation, otherwise False
    '''
    
    return bool( issubclass(obj.__class__, ForeignRelation) )

class RelationIntegrityError(Exception):
    '''
        RelationIntegrityError - Exception raised when there is an integrity violation
            in the relation. For example, if a one-to-one relation returns multiple results.
    '''

class OneToOneRelation(ForeignRelation):
    '''
        OneToOneRelation - A foreign relation where a foreign key on one model references one instance of another model
    '''

    def __init__(self, fkFieldName, relatedType, relatedFieldName=None):
        '''
            __init__ - Create a OneToOneRelation

                @param fkFieldName <str> - The field on the local model which references the foreign model

                @param relatedType <DatabaseModel type> - The foreign model type

                @param relatedFieldName <None/str> Default None - If None, will use the primary key
                    of #relatedType as the other end of the relation. Otherwise, provide an explicit field name.
        '''
        
        self.fkFieldName = fkFieldName
        self.relatedType = relatedType

        if relatedFieldName:
            self.relatedFieldName = relatedFieldName
        else:
            self.relatedFieldName = relatedType.PRIMARY_KEY


    def getRelated(self, sourceObj):
        '''
            getRelated - Follow the relation on #sourceObj and return the related object

                @param sourceObj <DatabaseModel instance> - The instance of the current model

                @return <None/DatabaseModel> - None if no related object found, otherwise the related object.
        '''
        
        fk = getattr(sourceObj, self.fkFieldName)

        if not fk:
            return None

        filterArgs = { self.relatedFieldName : fk }

        relatedObjs = self.relatedType.filter(**filterArgs)

        if len(relatedObjs) > 1:
            raise RelationIntegrityError('Expected a one-to-one relation from %s.%s -> %s.%s but got %d results on foreign key %s' % \
                ( sourceObj.__class__.__name__, self.fkFieldName, self.relatedType.__class__.__name__, self.relatedFieldName, len(relatedObjs), fk)
            )

        if relatedObjs:
            return relatedObjs[0]

        return None


class OneToManyRelation(ForeignRelation):
    '''
        OneToManyRelation - A relation where one field on the current object references one or more records of another type
    '''

    def __init__(self, fkFieldName, relatedType, relatedFieldName):
        '''
            __init__ - Create this relation object

                @param fkFieldName <str> - The field on the current model that is being referenced

                @param relatedType <DatabaseModel type> - The model that references this model on #fkFieldName

                @param relatedFieldName <str> - The field on the foreign model which references #fkFieldName
        '''
        
        self.fkFieldName = fkFieldName
        self.relatedType = relatedType

        if relatedFieldName:
            self.relatedFieldName = relatedFieldName
        else:
            self.relatedFieldName = relatedType.PRIMARY_KEY


    def getRelated(self, sourceObj):
        '''
            getRelated - Return all related objects based off this relation

                @param sourceObj <DatabaseModel instance> - The instance of current object

                @return list<DatabaseModel instance> - All instances of foreign model referenced
        '''
        
        fk = getattr(sourceObj, self.fkFieldName)

        if not fk:
            # TODO: What to return here? None or empty list?
            return []

        filterArgs = { self.relatedFieldName : fk }

        relatedObjs = self.relatedType.filter(**filterArgs)

        return relatedObjs

# TODO: ManyToOne?
