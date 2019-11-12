# LDAP Role Management

_TODO: move this text to docs.scylladb.com_

Scylla can use LDAP to manage which roles a user has.  This behavior
is triggered by setting the "role_manager" entry in `scylla.yaml` to
"com.scylladb.auth.LDAPRoleManager".  When this role manager is
chosen, Scylla forbids "GRANT" and "REVOKE" role statements.  Instead,
all users get their roles from the content of an LDAP directory.

(Note that Scylla still allows "GRANT" and "REVOKE" _permissions_
statements, such as "GRANT __permission__ ON __resource__ TO
__grantee__", which are handled by authorizer, not role manager.  That
way permissions can still be granted to and revoked from LDAP-managed
roles.)

Whenever a Scylla user authenticates to Scylla, a query is sent to the
LDAP server, whose response sets the user's roles for that login
session.  The user keeps the granted roles until logout; any
subsequent changes in LDAP become effective only at the user's next
login to Scylla.

The precise form of the LDAP query is configured by Scylla
administrator in `scylla.yaml`.  This configuration takes the form of
a query template similar to MongoDB's
"security.ldap.authz.queryTemplate".  In `scylla.yaml`, this parameter
is named "ldap_url_template".  The value of "ldap_url_template" should
be a valid LDAP URL (e.g., as returned by the `ldapurl` utility from
OpenLDAP) representing an LDAP query that returns entries for all the
user's roles.  Like MongoDB, Scylla will replace the text "{USER}" in
the URL with the Scylla username before querying LDAP.

For example, this template URL will query LDAP server at
localhost:5000 for all entries under "base_dn" that list the user's
username as one of their "uniqueMember" attribute values:

`ldap://localhost:5000/base_dn?cn?sub?(uniqueMember={USER})`

After Scylla queries LDAP and obtains the resulting entries, it looks
for a particular attribute in each entry and uses that attribute's
value as a Scylla role this user will have.  The name of this
attribute can be configured in `scylla.yaml` by setting the
"ldap_attr_role" parameter there.

When the LDAP query returns multiple entries, multiple roles will be
granted to the user.  Each role must already exist in Scylla, created
via the "CREATE ROLE" CQL command beforehand.

For example, if the LDAP query returns the following results:
```
# extended LDIF
# 
# LDAPv3

# role1, example.com
dn: cn=role1,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role1
scyllaName: sn1
uniqueMember: uid=jsmith,ou=People,dc=example,dc=com
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com

# role2, example.com
dn: cn=role2,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role2
scyllaName: sn2
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com

# role3, example.com
dn: cn=role3,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role3
uniqueMember: uid=jdoe,ou=People,dc=example,dc=com
```

and "ldap_attr_role" is set to "cn", then the resulting roleset will
be { role1, role2, role3 } (assuming, of course, that these roles
already exist in Scylla).  However, if "ldap_attr_role" is set to
"scyllaName", then the resulting roleset will be { sn1, sn2 }.  If an
LDAP entry does not have the "ldap_attr_role" attribute, it is simply
ignored.

Before Scylla attempts to query the LDAP server, it first performs an
LDAP bind operation, to gain access to the directory information.
Scylla executes a simple bind with credentials configured in
`scylla.yaml`.  The parameters "ldap_bind_dn" and "ldap_bind_passwd"
must contain, respectively, the distinguished name and password that
Scylla uses to perform the simple bind.
