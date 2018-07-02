'''
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE


    objs - Various utility classes
'''

__all__ = ('DictObj', 'IgnoreParameterType', 'IgnoreParameter', 'UseGlobalSetting')


class DictObj(dict):
    '''
        DictObj - A dictionary that supports dot-access the same as array-access
                    for reads, but not for writes.
    '''


    def __getattribute__(self, attrName):
        '''
            __getattribute__ - Called on dot-access.
                
                Searches if a key exists within this dict with the given name,
                  falling back to object-access
        '''
        if attrName in self:
            return self[attrName]

        return object.__getattribute__(self, attrName)


class IgnoreParameterType(object):
    '''
        IgnoreParameterType - A special type for a singleton, #IgnoreParameter,

            which can be used as a default "ignore this parameter" when None/False
            are valid values.

          Because it is intended as a singleton type, comparisons are such that
             IgnoreParameterType only equals other IgnoreParameterType objects (or the type itself)
    '''

    def __init__(self, reprName='IgnoreParameter'):
        self.reprName = reprName

    def __eq__(self, other):
        '''
            __eq__ - Test equality. Comparing against any other IgnoreParameterType object
                        or the type itself is True, anything else is False.

                     @param other <???> - The variable being compared

                     @return <bool> - True if same type or is the type itself, otherwise False
        '''

        if isinstance(other, IgnoreParameterType) or other == IgnoreParameterType:
            return True
        return False

    def __ne__(self, other):
        '''
            __ne__ - Test inequality.

                @see IgnoreParameterType.__eq__
        '''

        return not IgnoreParameterType.__eq__(self, other)

    def __bool__(self):
        '''
            __bool__ - Typecast to a boolean. Returns False.

                @return <bool> - False
        '''
        return False

    def __repr__(self):
        return self.reprName

# IgnoreParameter - Singleton instance of IgnoreParameterType
IgnoreParameter = IgnoreParameterType('IgnoreParameter')

# UseGlobalSetting - Singleton instance of IgnoreParameterType but with a different name
UseGlobalSetting = IgnoreParameterType('UseGlobalSetting')
