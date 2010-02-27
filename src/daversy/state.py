import os, sys, re, tempfile, ConfigParser
from os   import path
from lxml import etree, _elementpath
import providers
from daversy.db import Database

#############################################################################

class FileState:
    def can_load(self, location):
        return path.isfile(location)

    def can_save(self, location):
        return ':' not in location or re.match('^[a-z]:', location, re.I)

    def load(self, location, filters = {}):
        document = etree.parse(location)
        db = self._detect_database(document.getroot().tag)
        if not db:
            raise LookupError('Unable to detect the provider')

        self._setup(db)

        schema   = etree.XMLSchema( etree.parse(db.__xsd__) )
        if not schema(document):
          message = 'Document does not comply with schema.\n\n%s' % schema.error_log
          raise etree.DocumentInvalid(message)

        return self._load_element(document.getroot(),
                                   db.__state__, filters)

    def save(self, state, location, info):
        db = self._lookup_database(state.__class__)
        if not db:
            raise LookupError('Unable to detect the provider')

        self._setup(db)

        output = file(location, 'w')
        dummy  = etree.Element('dummy')
        self._save_element(dummy, state)

        root = dummy[0]
        root.attrib['xmlns'] = db.__xmlns__

        etree.ElementTree(root).write(output, pretty_print=True)
        output.close()

    def save_sql(self, state, location, info, type='all'):
        db = self._lookup_database(state.__class__)
        if not db:
            raise LookupError('Unable to detect the provider')

        self._setup(db)

        sql = ''
        comment = []
        for key in state.SubElements:
            for item in state[key].values():
                builder = self.db_mapping[item.__class__]
                sql += builder.createSQL(item)
                if hasattr(builder, 'commentSQL'):
                    comment.extend(builder.commentSQL(item))

        commentSQL = "\n".join(comment)

        if type == 'all':
            sql += commentSQL
        elif type == 'comment':
            sql = commentSQL

        stream = file(location, 'w')
        stream.write(sql.encode('utf-8'))
        stream.close()

    def _detect_database(self, tag):
        match = re.match('^{([^}]+)}', tag)
        if not match:
            return None

        namespace = match.group(1)
        for db in Database.list():
            if namespace == db.__xmlns__:
                return db
        return None

    def _lookup_database(self, dbclass):
        for db in Database.list():
            if dbclass == db.__state__:
                return db
        return None

    def _setup(self, db):
        self.db_mapping, self.xml_mapping = {}, {}
        for builder in db.__builders__:
            self.db_mapping[builder.DbClass] = builder
            self.xml_mapping[builder.XmlTag] = "{%s}%s" % (db.__xmlns__,
                                                            builder.XmlTag)

    def _load_element(self, node, dbclass, filters):
        if not self.db_mapping.has_key(dbclass):
            return None

        builder = self.db_mapping[dbclass]
        if not node.tag == self.xml_mapping[builder.XmlTag]:
            return None

        # load attributes
        object = builder.DbClass()
        if hasattr(builder, 'PropertyList'):
            for prop in builder.PropertyList.values():
                if prop.exclude:
                    continue
                object[prop.name] = prop.default
                if prop.name in node.attrib:
                    object[prop.name] = node.get(prop.name) or prop.default
                    continue
                if prop.cdata:
                    if not hasattr(object, 'SubElements'):
                        object[prop.name] = node.text or prop.default
                    else:
                        for sub in node:
                            tag = '}' in sub.tag and sub.tag[sub.tag.index('}')+1:] or sub.tag
                            if tag == prop.name:
                                object[prop.name] = sub.text or prop.default

        # check if it is excluded
        if not is_allowed( object, filters.get(builder.XmlTag) ):
            return None

        # load sub-elements
        if hasattr(object, 'SubElements'):
            for key, dbclass in object.SubElements.items():
                for child in node:
                    childObject = self._load_element(child, dbclass, filters)
                    if childObject:
                        object[key][childObject.name] = childObject

        return object

    def _save_element(self, node, object):
        builder = self.db_mapping[object.__class__]
        subnode = etree.SubElement(node, builder.XmlTag)

        # save attributes
        if hasattr(builder, 'PropertyList'):
            for key, value in builder.PropertyList.items():
                if not value.exclude and object[value.name]:
                    if not value.cdata:
                        subnode.attrib[value.name] = object[value.name]
                    else:
                        if not hasattr(object, 'SubElements'):
                            subnode.text = etree.CDATA(object[value.name])
                        else:
                            cdata = etree.SubElement(subnode, value.name)
                            cdata.text = etree.CDATA(object[value.name])

        # save sub-elements
        if hasattr(object, 'SubElements'):
            for key in object.SubElements.keys():
                for item in object[key].values():
                    self._save_element(subnode, item)

#############################################################################

class DatabaseState(object):
    def can_load(self, location):
        db, params = self._detect_database(location)
        return db and db.__conn__

    def can_save(self, location):
        return False

    def _detect_database(self, location):
        keys = location.split(':')
        if len(keys) < 2:
            return None, None

        return Database.get(keys[0]), keys[1:]

    def load(self, location, filters = {}):
        if not self.can_load(location):
            return None

        db, params = self._detect_database(location)

        state      = db.__state__()
        connection = db.__conn__(params)

        for builder in db.__builders__:
            print "Extracting %s" % builder.DbClass.__name__
            if hasattr(builder, 'Query'):
                self._load_object(connection.cursor(), state,
                                  builder, filters.get(builder.XmlTag))
            if hasattr(builder, 'customQuery'):
                builder.customQuery(connection.cursor(), state, builder)

        connection.close()
        return state

    def _load_object(self, cursor, state, builder, filters):
        cursor.execute(builder.Query)

        # find the column position and names
        columnNames = [c[0] for c in cursor.description]
        columnIndex = dict(map(None, columnNames, range(len(columnNames))))

        for row in cursor:
            # set the object properties
            newObject = builder.DbClass()
            for name in columnNames:
                setProperty = builder.PropertyList[name]
                setProperty(newObject, row[columnIndex[name]])

            if is_allowed(newObject, filters):
                builder.addToState(state, newObject)

        cursor.close()

class VersionControlState(object):
    def can_load(self, location):
        provider, params = self._detect_provider(location)
        return provider and provider.available()

    def can_save(self, location):
        return self.can_load(location)

    def _detect_provider(self, location):
        keys = location.split(':')
        if len(keys) < 2:
            return None, None

        return providers.VERSIONCONTROL.get(keys[0]), keys[1:]

    def load(self, location, filters = {}):
        if not self.can_load(location):
            return None

        provider, params = self._detect_provider(location)
        data = provider.load_file(params, None)
        if not data:
            return None
        return FileState().load(data, filters)

    def save(self, state, location, info):
        if not self.can_save(location):
            return None

        provider, params = self._detect_provider(location)
        handle, temp_location = tempfile.mkstemp()
        os.close(handle)
        FileState().save(state, temp_location, info)
        provider.save_file(params, temp_location, info)

    def save_sql(self, state, location, info, type):
        if not self.can_save(location):
            return None

        provider, params = self._detect_provider(location)
        handle, temp_location = tempfile.mkstemp()
        os.close(handle)
        FileState().save_sql(state, temp_location, info, type)
        provider.save_file(params, temp_location, info)

#############################################################################

PROVIDERS = [ FileState(), DatabaseState(), VersionControlState() ]

#############################################################################

def create_filter(filename, included_tags, excluded_tags):
    config = ConfigParser.ConfigParser()
    if not config.read(filename):
        return None

    parse_tag = lambda x: [t.strip() for t in x.split(',') if t.strip()]

    included, excluded = parse_tag(included_tags), parse_tag(excluded_tags)
    filters = {}
    for section in config.sections():
        include_list, exclude_list = [], []
        for name, tag_spec in config.items(section):
            for tag in parse_tag(tag_spec):
                if tag in included:
                    include_list.append( re.compile('^%s$' % name, re.I) )
                if tag in excluded:
                    exclude_list.append( re.compile('^%s$' % name, re.I) )

        filters[section] = (include_list, exclude_list)

    return filters

def is_allowed(object, filters):
    if filters is None or not object.has_key('name'):
        return True

    include_list, exclude_list = filters

    allowed = False
    for filter in include_list:
        if filter.match(object['name']):
            allowed = True

    for filter in exclude_list:
        if filter.match(object['name']):
            allowed = False

    return allowed
