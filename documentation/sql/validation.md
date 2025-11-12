# **SQL Query Validation at Compile Time using macros**

## **Table of Contents**

1. [Overview](#overview-)
2. [Validations Performed](#validations-performed)
3. [Validation Examples with Error Messages](#validation-examples-with-error-messages)
   - [1. SELECT * Validation](#1-select--validation)
   - [2. Missing Fields Validation](#2-missing-fields-validation)
   - [3. Unknown Fields Validation](#3-unknown-fields-validation)
   - [4. Nested Objects Validation](#4-nested-objects-validation)
   - [5. Nested Collections Validation](#5-nested-collections-validation)
   - [6. Type Validation](#6-type-validation)
4. [Best Practices](#best-practices)
5. [Debug Configuration](#debug-configuration)

---

## **Overview** 🎯

The Elasticsearch SQL client integrates a **compile-time validation system** that automatically verifies your SQL queries are compatible with the Scala case classes used for deserialization. This validation detects errors **before execution**, ensuring consistency between your queries and your data model.

### **Benefits** ✅

- ✅ **Early error detection**: Issues are identified at compile time, not in production
- ✅ **Safe refactoring**: Renaming or removing a field generates a compilation error
- ✅ **Living documentation**: Case classes document the data structure
- ✅ **Strong typing**: Guarantees consistency between SQL and Scala
- ✅ **Explicit error messages**: Clear guidance on how to fix issues

---

## **Validations Performed**

| Validation             | Description                                            | Level      |
|------------------------|--------------------------------------------------------|------------|
| **SELECT * Rejection** | Prohibits `SELECT *` to ensure compile-time validation | ❌ ERROR    |
| **Required Fields**    | Verifies that all required fields are selected         | ❌ ERROR    |
| **Unknown Fields**     | Detects fields that don't exist in the case class      | ⚠️ WARNING |
| **Nested Objects**     | Validates the structure of nested objects              | ❌ ERROR    |
| **Nested Collections** | Validates the use of UNNEST for collections            | ❌ ERROR    |
| **Type Compatibility** | Checks compatibility between SQL and Scala types       | ❌ ERROR    |

---

## **Validation Examples with Error Messages**

### **1. SELECT \* Validation**

#### **❌ Error Example**

```scala
case class Product(
  id: String,
  name: String,
  price: Double
)

// ❌ ERROR: SELECT * is not allowed
client.searchAs[Product]("SELECT * FROM products")
```

#### **📋 Exact Error Message**

```
❌ SELECT * is not allowed with compile-time validation.

Query: SELECT * FROM products

Reason:
  • Cannot validate field existence at compile-time
  • Cannot validate type compatibility at compile-time
  • Schema changes will break silently at runtime

Solution:
  1. Explicitly list all required fields for Product:
     SELECT id, name, price FROM ...

  2. Use the *Unchecked() variant for dynamic queries:
     searchAsUnchecked[Product](SQLQuery("SELECT * FROM ..."))

Best Practice:
  Always explicitly select only the fields you need.
```

#### **✅ Solution**

```scala
// ✅ CORRECT: Explicitly select all fields
client.searchAs[Product]("SELECT id, name, price FROM products")
```

---

### **2. Missing Fields Validation**

#### **2.1. Missing Simple Field**

##### **❌ Error Example**

```scala
case class User(
  id: String,
  name: String,
  email: String
)

// ❌ ERROR: The 'email' field is missing
client.searchAs[User]("SELECT id, name FROM users")
```

##### **📋 Exact Error Message**

```
❌ SQL query does not select the required field: email

Example query:
SELECT id, name, email FROM ...

To fix this, either:
  1. Add it to the SELECT clause
  2. Make it Option[T] in the case class
  3. Provide a default value in the case class definition
```

##### **✅ Solutions**

**Option 1: Add the missing field**
```scala
// ✅ CORRECT
client.searchAs[User]("SELECT id, name, email FROM users")
```

**Option 2: Make the field optional**
```scala
case class User(
  id: String,
  name: String,
  email: Option[String] = None  // ✅ Optional field
)

// ✅ CORRECT
client.searchAs[User]("SELECT id, name FROM users")
```

**Option 3: Provide a default value**
```scala
case class User(
  id: String,
  name: String,
  email: String = ""  // ✅ Default value
)

// ✅ CORRECT
client.searchAs[User]("SELECT id, name FROM users")
```

---

#### **2.2. Field with Suggestion (Did You Mean?)**

##### **❌ Error Example**

```scala
case class Product(
  id: String,
  name: String,
  price: Double
)

// ❌ ERROR: Typo in 'price' -> 'pric'
client.searchAs[Product]("SELECT id, name, pric FROM products")
```

##### **📋 Exact Error Message**

```
❌ SQL query does not select the required field: price
You have selected unknown field "pric", did you mean "price"?

Example query:
SELECT id, name, price FROM ...

To fix this, either:
  1. Add it to the SELECT clause
  2. Make it Option[T] in the case class
  3. Provide a default value in the case class definition
```

##### **✅ Solution**

```scala
// ✅ CORRECT: Fix the typo
client.searchAs[Product]("SELECT id, name, price FROM products")
```

---

### **3. Unknown Fields Validation**

#### **⚠️ Warning Example**

```scala
case class User(
  id: String,
  name: String,
  email: String
)

// ⚠️ WARNING: The 'age' field doesn't exist in User
client.searchAs[User]("SELECT id, name, email, age FROM users")
```

#### **📋 Exact Warning Message**

```
⚠️ SQL query selects fields that don't exist in User:
age

Available fields: id, name, email

Note: These fields will be ignored during deserialization.
```

#### **💡 Behavior**

- ✅ **The code compiles successfully**
- ⚠️ **A warning is displayed** to inform about the unknown field
- 🔄 **During deserialization**, the unknown field is **silently ignored**
- 📦 **The JSON response** contains the field, but it's not mapped to the case class

#### **✅ Solutions**

**Option 1: Remove the unknown field**
```scala
// ✅ CORRECT: Only select existing fields
client.searchAs[User]("SELECT id, name, email FROM users")
```

**Option 2: Add the field to the case class**
```scala
case class User(
  id: String,
  name: String,
  email: String,
  age: Option[Int] = None  // ✅ Field added
)

// ✅ CORRECT
client.searchAs[User]("SELECT id, name, email, age FROM users")
```

---

### **4. Nested Objects Validation**

#### **4.1. Nested Object with Individual Field Selection**

##### **❌ Error Example**

```scala
case class Address(
  street: String,
  city: String,
  country: String
)

case class User(
  id: String,
  name: String,
  address: Address
)

// ❌ ERROR: Selecting nested fields without UNNEST
client.searchAs[User](
  "SELECT id, name, address.street, address.city, address.country FROM users"
)
```

##### **📋 Exact Error Message**

```
❌ Nested object field 'address' cannot be deserialized correctly.

❌ Problem:
   You are selecting nested fields individually:
   address.street, address.city, address.country

   Elasticsearch will return flat fields like:
   { "address.street": "value1", "address.city": "value2", "address.country": "value3" }

   But Jackson needs a structured object like:
   { "address": {"street": "value1", "city": "value2", "country": "value3"} }

✅ Solution 1: Select the entire nested object (recommended)
   SELECT address FROM ...

✅ Solution 2: Use UNNEST (if you need to filter or join on nested fields)
   SELECT address.street, address.city, address.country
   FROM ...
   JOIN UNNEST(....address) AS address

📚 Note: This applies to ALL nested objects, not just collections.
```

##### **✅ Solutions**

**Option 1: Select the complete object (RECOMMENDED)**
```scala
// ✅ CORRECT: Select the entire object
client.searchAs[User]("SELECT id, name, address FROM users")
```

**Elasticsearch Response**:
```json
{
  "id": "u1",
  "name": "Alice",
  "address": {
    "street": "123 Main St",
    "city": "Wonderland",
    "country": "Fictionland"
  }
}
```

**Option 2: Use UNNEST**
```scala
// ✅ CORRECT: Use UNNEST for filtering/joining
client.searchAs[User](
  """SELECT id, name, address.street, address.city, address.country
     FROM users
     JOIN UNNEST(users.address) AS address
     WHERE address.city = 'Wonderland'"""
)
```

---

#### **4.2. Missing Nested Object**

##### **❌ Error Example**

```scala
case class Address(
  street: String,
  city: String,
  country: String
)

case class User(
  id: String,
  name: String,
  address: Address  // ❌ Required field not selected
)

// ❌ ERROR: The 'address' object is not selected
client.searchAs[User]("SELECT id, name FROM users")
```

##### **📋 Exact Error Message**

```
❌ SQL query does not select the required field: address

Example query:
SELECT id, name, address FROM ...

To fix this, either:
  1. Add it to the SELECT clause
  2. Make it Option[T] in the case class
  3. Provide a default value in the case class definition
```

##### **✅ Solutions**

**Option 1: Add the missing field**
```scala
// ✅ CORRECT
client.searchAs[User]("SELECT id, name, address FROM users")
```

**Option 2: Make the object optional**
```scala
case class User(
  id: String,
  name: String,
  address: Option[Address] = None  // ✅ Optional object
)

// ✅ CORRECT
client.searchAs[User]("SELECT id, name FROM users")
```

---

### **5. Nested Collections Validation**

#### **5.1. Nested Collection with Individual Field Selection without UNNEST**

##### **❌ Error Example**

```scala
case class Child(
  name: String,
  age: Int
)

case class Parent(
  id: String,
  name: String,
  children: List[Child]
)

// ❌ ERROR: Selecting nested fields without UNNEST
client.searchAs[Parent](
  "SELECT id, name, children.name, children.age FROM parent"
)
```

##### **📋 Exact Error Message**

```
❌ Collection field 'children' cannot be deserialized correctly.

❌ Problem:
   You are selecting nested fields without using UNNEST:
   children.name, children.age

   Elasticsearch will return flat arrays like:
   { "children.name": ["Alice", "Bob"], "children.age": [10, 12] }

   But Jackson needs structured objects like:
   { "children": [{"name": "Alice", "age": 10}, {"name": "Bob", "age": 12}] }

✅ Solution 1: Select the entire collection (recommended for simple queries)
   SELECT children FROM ...

✅ Solution 2: Use UNNEST for precise field selection (recommended for complex queries)
   SELECT children.name, children.age
   FROM ...
   JOIN UNNEST(....children) AS children

📚 Documentation:
   https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html
```

##### **✅ Solutions**

**Option 1: Select the complete collection (RECOMMENDED)**
```scala
// ✅ CORRECT: Select the entire collection
client.searchAs[Parent]("SELECT id, name, children FROM parent")
```

**Elasticsearch Response**:
```json
{
  "id": "p1",
  "name": "Parent Name",
  "children": [
    {"name": "Alice", "age": 10},
    {"name": "Bob", "age": 12}
  ]
}
```

**Option 2: Use UNNEST**
```scala
// ✅ CORRECT: Use UNNEST for filtering/joining
client.searchAs[Parent](
  """SELECT id, name, children.name, children.age
     FROM parent
     JOIN UNNEST(parent.children) AS children
     WHERE children.age > 10"""
)
```

---

#### **5.2. Missing Nested Collection**

##### **❌ Error Example**

```scala
case class Child(
  name: String,
  age: Int
)

case class Parent(
  id: String,
  name: String,
  children: List[Child]  // ❌ Required collection not selected
)

// ❌ ERROR: The 'children' collection is not selected
client.searchAs[Parent]("SELECT id, name FROM parent")
```

##### **📋 Exact Error Message**

```
❌ SQL query does not select the required field: children

Example query:
SELECT id, name, children FROM ...

To fix this, either:
  1. Add it to the SELECT clause
  2. Make it Option[T] in the case class
  3. Provide a default value in the case class definition
```

##### **✅ Solutions**

**Option 1: Add the missing collection**
```scala
// ✅ CORRECT
client.searchAs[Parent]("SELECT id, name, children FROM parent")
```

**Option 2: Make the collection optional**
```scala
case class Parent(
  id: String,
  name: String,
  children: Option[List[Child]] = None  // ✅ Optional collection
)

// ✅ CORRECT
client.searchAs[Parent]("SELECT id, name FROM parent")
```

---

### **6. Type Validation**

#### **6.1. Type Incompatibility**

##### **❌ Error Example**

```scala
case class Product(
  id: String,
  name: String,
  stock: Int  // ❌ Wrong type (should be Long)
)

// ❌ ERROR: The 'stock' field is cast to BIGINT in SQL
client.searchAs[Product]("SELECT id, name, stock::BIGINT FROM products")
```

##### **📋 Exact Error Message**

```
Type mismatch for field 'stock': SQL type BIGINT is incompatible with Scala type Int
Expected one of: Long, BigInt, Option[Long], Option[BigInt]
```

##### **✅ Solution**

```scala
case class Product(
  id: String,
  name: String,
  stock: Long  // ✅ Correct type
)

// ✅ CORRECT
client.searchAs[Product]("SELECT id, name, stock::BIGINT FROM products")
```

---

#### **6.2. Type Compatibility Table**

| SQL Type                | Compatible Scala Types                                                                         |
|-------------------------|------------------------------------------------------------------------------------------------|
| `TINYINT`               | `Byte`, `Short`, `Int`, `Long`, `Option[Byte]`, `Option[Short]`, `Option[Int]`, `Option[Long]` |
| `SMALLINT`              | `Short`, `Int`, `Long`, `Option[Short]`, `Option[Int]`, `Option[Long]`                         |
| `INT`                   | `Int`, `Long`, `Option[Int]`, `Option[Long]`                                                   |
| `BIGINT`                | `Long`, `BigInt`, `Option[Long]`, `Option[BigInt]`                                             |
| `DOUBLE`, `REAL`        | `Double`, `Float`, `Option[Double]`, `Option[Float]`                                           |
| `VARCHAR`               | `String`, `Option[String]`                                                                     |
| `CHAR`                  | `String`, `Char`, `Option[String]`, `Option[Char]`                                             |
| `BOOLEAN`               | `Boolean`, `Option[Boolean]`                                                                   |
| `TIME`                  | `java.time.LocalTime`, `java.time.Instant`                                                     |
| `DATE`                  | `java.time.LocalDate`, `java.time.Instant`, `java.util.Date`                                   |
| `DATETIME`, `TIMESTAMP` | `java.time.LocalDateTime`, `java.time.ZonedDateTime`, `java.time.Instant`                      |
| `STRUCT`                | Case Class                                                                                     |

---

## **Best Practices**

### **1. Always Explicitly Select Fields**

❌ **Avoid**:
```scala
client.searchAs[Product]("SELECT * FROM products")
```

✅ **Prefer**:
```scala
client.searchAs[Product]("SELECT id, name, price FROM products")
```

---

### **2. Use UNNEST for Nested Collections and Objects**

❌ **Avoid**:

```scala
client.searchAs[Parent]("SELECT id, children.name FROM parent")
```

✅ **Prefer**:

```scala
// Option 1: Select the complete collection
client.searchAs[Parent]("SELECT id, children FROM parent")

// Option 2: Use UNNEST for filtering
client.searchAs[Parent](
  """SELECT id, children.name
     FROM parent
     JOIN UNNEST(parent.children) AS children"""
)
```

---

### **3. Make Fields Optional Only When Necessary**

✅ **Simple Fields**: Can be made optional if not required

```scala
case class User(
  id: String,
  name: String,
  email: Option[String] = None  // ✅ OK for simple fields
)
```

⚠️ **Nested Objects/Collections**: Don't make optional to bypass validation errors

```scala
// ❌ BAD PRACTICE: Making nested optional to avoid error
case class User(
  id: String,
  name: String,
  address: Option[Address] = None  // ❌ Avoid if 'address' is required
)

// ✅ GOOD PRACTICE: Fix the SQL query
client.searchAs[User]("SELECT id, name, address FROM users")
```

---

### **4. Use Default Values with Caution**

✅ **For Simple Fields**:
```scala
case class Product(
  id: String,
  name: String,
  price: Double = 0.0,  // ✅ OK
  inStock: Boolean = true  // ✅ OK
)
```

❌ **For Nested Objects** (avoid):
```scala
case class User(
  id: String,
  name: String,
  address: Address = Address("", "", "")  // ❌ Avoid
)
```

---

## **Debug Configuration**

### **Enable Debug Mode**

```scala
// In build.sbt or command line
System.setProperty("elastic.sql.debug", "true")
```

### **Debug Output Example**

```
================================================================================
🔍 Starting SQL Query Validation
================================================================================
📝 Extracted SQL: SELECT id, name, address.street FROM users
🔍 Parsed fields: id, name, address.street
🔍 Unnested collections: 
📋 Required fields for User (prefix=''): id, name, address
🔍 Checking field: id (type: String, optional: false, hasDefault: false)
✅ Field 'id' is directly selected
🔍 Checking field: name (type: String, optional: false, hasDefault: false)
✅ Field 'name' is directly selected
🔍 Checking field: address (type: Address, optional: false, hasDefault: false)
🏗️ Field 'address' is a nested object (non-collection)
❌ ERROR: Nested object field 'address' cannot be deserialized correctly.
```

---

## **Validation Rules Summary**

| Rule                                 | Behavior                                              | Level      |
|--------------------------------------|-------------------------------------------------------|------------|
| **SELECT \***                        | Prohibited                                            | ❌ ERROR    |
| **Required field missing**           | Must be added, made optional, or have a default value | ❌ ERROR    |
| **Unknown field**                    | Warning (ignored during deserialization)              | ⚠️ WARNING |
| **Nested object without UNNEST**     | Must select complete object or use UNNEST             | ❌ ERROR    |
| **Nested collection without UNNEST** | Must select complete collection or use UNNEST         | ❌ ERROR    |
| **Type incompatibility**             | Must use a compatible Scala type                      | ❌ ERROR    |

---

**This compile-time validation ensures the robustness and maintainability of your code! 🚀✅**
