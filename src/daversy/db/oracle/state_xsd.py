from StringIO import StringIO

schema = StringIO("""
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns="http://www.daversy.org/schemas/state/oracle"
            targetNamespace="http://www.daversy.org/schemas/state/oracle"
            elementFormDefault="qualified" attributeFormDefault="unqualified">
  <!--                        -->
  <!-- basic name/value types -->
  <!--                        -->
  <xsd:simpleType name="NameType">
    <xsd:restriction base="xsd:normalizedString">
      <xsd:pattern value="[a-zA-Z0-9_#$=]{1,30}" />
    </xsd:restriction>
  </xsd:simpleType>
  <xsd:simpleType name="ContentType">
    <xsd:restriction base="xsd:string">
      <xsd:whiteSpace value="preserve" />
    </xsd:restriction>
  </xsd:simpleType>
  <!--
       workaround for a bug in libxml2: integer is supposed to be infinite.
       http://www.w3.org/TR/xmlschema-2/#integer
  -->
  <xsd:simpleType name="BigInteger">
    <xsd:restriction base="xsd:normalizedString">
      <xsd:pattern value="[-\+]?[0-9]+"/>
    </xsd:restriction>
  </xsd:simpleType>
  <!--                  -->
  <!-- basic enum types -->
  <!--                  -->
  <xsd:simpleType name="ColumnTypeEnum">
    <xsd:restriction base="xsd:normalizedString">
      <xsd:enumeration value="varchar2" />
      <xsd:enumeration value="number" />
      <xsd:enumeration value="date" />
      <xsd:enumeration value="char" />
      <xsd:enumeration value="long" />
      <xsd:enumeration value="long raw" />
      <xsd:enumeration value="raw" />
      <xsd:enumeration value="rowid" />
      <xsd:enumeration value="float" />
      <xsd:enumeration value="integer" />
      <xsd:enumeration value="nchar" />
      <xsd:enumeration value="nvarchar2" />
      <xsd:enumeration value="blob" />
      <xsd:enumeration value="clob" />
      <xsd:enumeration value="nclob" />
      <xsd:enumeration value="bfile" />
      <xsd:enumeration value="urowid" />
      <xsd:enumeration value="timestamp" />
      <xsd:enumeration value="timestamp with time zone" />
      <xsd:enumeration value="timestamp with local time zone" />
      <xsd:enumeration value="interval year to month" />
      <xsd:enumeration value="interval day to second" />
      <xsd:enumeration value="binary_float" />
      <xsd:enumeration value="binary_double" />
      <xsd:enumeration value="undefined" />
    </xsd:restriction>
  </xsd:simpleType>
  <xsd:simpleType name="IndexSortEnum">
    <xsd:restriction base="xsd:normalizedString">
      <xsd:enumeration value="asc" />
      <xsd:enumeration value="desc" />
    </xsd:restriction>
  </xsd:simpleType>
  <xsd:simpleType name="ReferentialDeleteEnum">
    <xsd:restriction base="xsd:normalizedString">
      <xsd:enumeration value="no action" />
      <xsd:enumeration value="cascade" />
      <xsd:enumeration value="set null" />
    </xsd:restriction>
  </xsd:simpleType>
  <xsd:simpleType name="TriggerObjectEnum">
      <xsd:restriction base="xsd:normalizedString">
          <xsd:enumeration value="table" />
          <xsd:enumeration value="view" />
      </xsd:restriction>
  </xsd:simpleType>
  <xsd:simpleType name="DeferTypeEnum">
      <xsd:restriction base="xsd:normalizedString">
          <xsd:enumeration value="immediate" />
          <xsd:enumeration value="deferred" />
      </xsd:restriction>
  </xsd:simpleType>
  <!--                  -->
  <!-- basic data types -->
  <!--                  -->
  <xsd:complexType name="IndexColumnType">
    <xsd:attribute name="name" type="xsd:string" use="required" />
    <xsd:attribute name="sort" type="IndexSortEnum" use="required" />
  </xsd:complexType>
  <xsd:complexType name="ConstraintColumnType">
    <xsd:attribute name="name" type="NameType" use="required" />
  </xsd:complexType>
  <xsd:complexType name="ColumnType">
    <xsd:attribute name="name" type="NameType" use="required" />
    <xsd:attribute name="type" type="ColumnTypeEnum" />
    <xsd:attribute name="custom-type" type="NameType" />
    <xsd:attribute name="length" type="xsd:positiveInteger" />
    <xsd:attribute name="precision" type="xsd:nonNegativeInteger" />
    <xsd:attribute name="scale" form="unqualified" type="xsd:nonNegativeInteger" />
    <xsd:attribute name="nullable" type="xsd:boolean" />
    <xsd:attribute name="notnull-defer-type" type="DeferTypeEnum" />
    <xsd:attribute name="check" type="xsd:string" />
    <xsd:attribute name="check-defer-type" type="DeferTypeEnum" />
    <xsd:attribute name="default-value" type="xsd:string" />
    <xsd:attribute name="comment" type="ContentType" />
  </xsd:complexType>
  <xsd:complexType name="GenericConstraintType">
    <xsd:sequence>
      <xsd:element name="constraint-column" type="ConstraintColumnType" minOccurs="1" maxOccurs="unbounded" />
    </xsd:sequence>
    <xsd:attribute name="name" type="NameType" />
    <xsd:attribute name="defer-type" type="DeferTypeEnum" />
  </xsd:complexType>
  <xsd:complexType name="CheckConstraintType">
    <xsd:simpleContent>
      <xsd:extension base="ContentType">
        <xsd:attribute name="name" type="NameType" />
        <xsd:attribute name="defer-type" type="DeferTypeEnum" />
        <xsd:attribute name="condition" type="ContentType" />
      </xsd:extension>
    </xsd:simpleContent>
  </xsd:complexType>
  <xsd:complexType name="SourceType">
    <xsd:simpleContent>
      <xsd:extension base="ContentType">
        <xsd:attribute name="name" type="NameType" use="required" />
        <xsd:attribute name="source" type="ContentType" />
        <xsd:attribute name="invalid" type="xsd:boolean" />
      </xsd:extension>
    </xsd:simpleContent>
  </xsd:complexType>
  <xsd:complexType name="ForeignKeyColumnType">
    <xsd:attribute name="name" type="NameType" use="required" />
    <xsd:attribute name="reference" type="NameType" use="required" />
  </xsd:complexType>
  <!--                       -->
  <!-- high level data types -->
  <!--                       -->
  <xsd:complexType name="TableType">
    <xsd:sequence>
      <xsd:element name="column" type="ColumnType" minOccurs="1" maxOccurs="unbounded" />
      <xsd:element name="primary-key" type="GenericConstraintType" minOccurs="0" maxOccurs="1" />
      <xsd:element name="unique-key" type="GenericConstraintType" minOccurs="0" maxOccurs="unbounded" />
      <xsd:element name="check-constraint" type="CheckConstraintType" minOccurs="0" maxOccurs="unbounded" />
    </xsd:sequence>
    <xsd:attribute name="name" type="NameType" use="required" />
    <xsd:attribute name="temporary" type="xsd:boolean" />
    <xsd:attribute name="on-commit-preserve-rows" type="xsd:boolean" />
    <xsd:attribute name="comment" type="ContentType" />
  </xsd:complexType>
  <xsd:complexType name="SequenceType">
    <xsd:attribute name="name" type="NameType" use="required" />
    <xsd:attribute name="increment-by" type="xsd:integer" />
    <xsd:attribute name="min-value" type="xsd:integer" />
    <xsd:attribute name="max-value" type="BigInteger" />
    <xsd:attribute name="cache-size" type="xsd:positiveInteger" />
    <xsd:attribute name="cycle-after-last" type="xsd:boolean" />
    <xsd:attribute name="guaranteed-order" type="xsd:boolean" />
  </xsd:complexType>
  <xsd:complexType name="IndexType">
    <xsd:sequence>
      <xsd:element name="index-column" type="IndexColumnType" minOccurs="1" maxOccurs="unbounded" />
    </xsd:sequence>
    <xsd:attribute name="name" type="NameType" use="required" />
    <xsd:attribute name="table-name" type="NameType" use="required" />
    <xsd:attribute name="unique" type="xsd:boolean" use="required" />
    <!-- TODO: what do the various index-types mean?
    <xsd:attribute name="index-type" type="xsd:string" default="normal" /> -->
  </xsd:complexType>
  <xsd:complexType name="ForeignKeyType">
      <xsd:sequence>
          <xsd:element name="foreign-key-column" type="ForeignKeyColumnType" minOccurs="1" maxOccurs="unbounded"/>
      </xsd:sequence>
      <xsd:attribute name="name" type="NameType" use="required" />
      <xsd:attribute name="table" type="NameType" use="required" />
      <xsd:attribute name="reference-table" type="NameType" use="required" />
      <xsd:attribute name="delete-rule" type="ReferentialDeleteEnum" use="required"/>
      <xsd:attribute name="defer-type" type="DeferTypeEnum" />
  </xsd:complexType>
  <xsd:complexType name="MaterializedViewType">
    <xsd:simpleContent>
      <xsd:extension base="ContentType">
        <xsd:attribute name="name" type="NameType" use="required" />
        <xsd:attribute name="invalid" type="xsd:boolean" />
        <!-- need to add enums here -->
        <xsd:attribute name="refresh-mode" type="xsd:string" />
        <xsd:attribute name="refresh-method" type="xsd:string" />
        <xsd:attribute name="build-mode" type="xsd:string" />
        <xsd:attribute name="source" type="ContentType" />
      </xsd:extension>
    </xsd:simpleContent>
  </xsd:complexType>
  <xsd:complexType name="ViewType">
    <xsd:sequence>
      <xsd:element name="definition" type="ContentType" minOccurs="0" maxOccurs="1" />
      <xsd:element name="column" type="ColumnType" minOccurs="0" maxOccurs="unbounded" />
    </xsd:sequence>
    <xsd:attribute name="name" type="NameType" use="required" />
    <xsd:attribute name="definition" type="ContentType" />
    <xsd:attribute name="comment" type="ContentType" />
  </xsd:complexType>
  <xsd:complexType name="TriggerType">
    <xsd:simpleContent>
      <xsd:extension base="ContentType">
        <xsd:attribute name="name" type="NameType" use="required" />
        <xsd:attribute name="object-type" type="TriggerObjectEnum" use="required" />
        <xsd:attribute name="object-name" type="NameType" use="required" />
        <xsd:attribute name="definition" type="ContentType" />
      </xsd:extension>
    </xsd:simpleContent>
  </xsd:complexType>
  <!--                    -->
  <!-- the database state -->
  <!--                    -->
  <xsd:element name="dvs-state">
    <xsd:complexType>
      <xsd:sequence>
        <xsd:element name="type" type="SourceType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="table" type="TableType" minOccurs="1" maxOccurs="unbounded" />
        <xsd:element name="sequence" type="SequenceType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="index" type="IndexType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="foreign-key" type="ForeignKeyType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="view" type="ViewType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="stored-procedure" type="SourceType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="function" type="SourceType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="package" type="SourceType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="trigger" type="TriggerType" minOccurs="0" maxOccurs="unbounded" />
        <xsd:element name="materialized-view" type="MaterializedViewType" minOccurs="0" maxOccurs="unbounded" />
      </xsd:sequence>
      <xsd:attribute name="name" type="xsd:normalizedString" use="required" />
    </xsd:complexType>
  </xsd:element>
</xsd:schema>
""")
