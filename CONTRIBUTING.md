# **Contributing to SoftClient4ES**

Thank you for your interest in contributing to SoftClient4ES! This document provides guidelines and best practices for contributing to the project.

---

## **Table of Contents**

1. [Getting Started](#getting-started)
2. [Development Environment Setup](#development-environment-setup)
3. [Project Structure](#project-structure)
4. [Development Workflow](#development-workflow)
5. [Coding Standards](#coding-standards)
6. [Testing Guidelines](#testing-guidelines)
7. [Pull Request Process](#pull-request-process)
8. [Documentation](#documentation)
9. [Release Process](#release-process)
10. [Community](#community)

---

## **Getting Started**

### **Ways to Contribute**

There are many ways to contribute to SoftClient4ES:

- 🐛 **Report Bugs**: Submit detailed bug reports with reproduction steps
- 💡 **Suggest Features**: Propose new features or improvements
- 📖 **Improve Documentation**: Fix typos, clarify explanations, add examples
- 🔧 **Fix Issues**: Pick up existing issues and submit pull requests
- ✨ **Add Features**: Implement new functionality
- 🧪 **Write Tests**: Improve test coverage
- 🔍 **Review Pull Requests**: Help review and test other contributors' work

### **Finding Issues to Work On**

Good first issues for newcomers are labeled with:
- `good-first-issue`: Suitable for first-time contributors
- `help-wanted`: Issues where we need community help
- `documentation`: Documentation improvements needed
- `bug`: Bug fixes needed
- `enhancement`: Feature enhancements

Browse open issues: [GitHub Issues](https://github.com/SOFTNETWORK-APP/SoftClient4ES/issues)

### **Before You Start**

1. **Check existing issues**: Make sure your bug/feature hasn't been reported/requested
2. **Discuss major changes**: For significant changes, open an issue first to discuss
3. **Read this guide**: Understand our development workflow and standards
4. **Set up your environment**: Follow the setup instructions below

---

## **Development Environment Setup**

### **Prerequisites**

- **JDK**: Java 17 or higher
- **Scala**: 2.12.x and 2.13.x
- **sbt**: 1.8.x or higher
- **Git**: Latest version
- **Docker**: For running Elasticsearch in tests
- **IDE**: IntelliJ IDEA (recommended) or VS Code with Metals

### **Fork and Clone**

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/softclient4es.git
cd softclient4es

# Add upstream remote
git remote add upstream https://github.com/SOFTNETWORK-APP/SoftClient4ES.git

# Verify remotes
git remote -v
```

### **Build the Project**

```bash
# Compile the project
sbt compile

# Run tests (requires Docker)
sbt test

# Generate coverage report
sbt clean coverage test coverageReport

# Check code formatting
sbt scalafmtCheck

# Format code
sbt scalafmtAll
```

### **IDE Setup**

#### **IntelliJ IDEA**

1. Install Scala plugin: `File > Settings > Plugins > Scala`
2. Import project: `File > Open` and select `build.sbt`
3. Wait for sbt to download dependencies
4. Enable scalafmt: `File > Settings > Editor > Code Style > Scala > Scalafmt`

#### **VS Code with Metals**

1. Install Metals extension
2. Open project folder
3. Wait for Metals to import build
4. Configure scalafmt in settings

### **Verify Setup**

```bash
# Run a simple test
sbt "testOnly *ElasticClientSpec"

# If successful, you're ready to contribute!
```

---

## **Project Structure**

```markdown
softclient4es/
├── bridge/                        # SQL to Elasticsearch DSL translation module
│   ├── src/
│   │   ├── main/scala/
│   │   │   └── app/softnetwork/elastic/sql/bridge
│   │   └── test/scala/
│   │       └── app/softnetwork/elastic/
│   │           └── sql/           # Unit tests
│   └── build.sbt
│
├── core/                          # Core abstractions and interfaces
│   ├── src/
│   │   ├── main/scala/
│   │   │   └── app/softnetwork/elastic/
│   │   │       └── client/        # Client API interfaces
│   │   └── test/scala/
│   │       └── app/softnetwork/elastic/
│   │           └── client/        # Unit tests
│   └── build.sbt
│
├── documentation/                 # Project documentation
│   ├── client/
│   ├── sql/
│   └── best-practices.md
│
├── es6/jest/                      # Jest Client (Elasticsearch 6.x)
│   ├── src/
│   │   ├── main/scala/
│   │   │   └── app/softnetwork/elastic/client/
│   │   │       ├── jest/          # Jest implementation
│   │   │       └── spi/           # Jest SPI implementation
│   │   └── test/scala/
│   │       └── app/softnetwork/elastic/
│   │           ├── client/        # Client tests
│   │           └── persistence/   # Persistence tests
│   └── build.sbt
│
├── es6,es7/rest/                  # Rest High Level client (Elasticsearch 6.x/7.x)
│   ├── src/
│   │   ├── main/scala/
│   │   │   └── app/softnetwork/elastic/client/
│   │   │       ├── rest/          # Rest High Level client implementation
│   │   │       └── spi/           # Rest High Level client SPI implementation
│   │   └── test/scala/
│   │       └── app/softnetwork/elastic/
│   │           ├── client/        # Rest High Level Client tests
│   │           └── persistence/   # Rest High Level Client Persistence tests
│   └── build.sbt
│
├── es8,es9/java/                  # Java Client (Elasticsearch 8.x/9.x)
│   ├── src/
│   │   ├── main/scala/
│   │   │   └── app/softnetwork/elastic/client/
│   │   │       ├── jest/          # Java Client implementation
│   │   │       └── spi/           # Java Client SPI implementation
│   │   └── test/scala/
│   │       └── app/softnetwork/elastic/
│   │           ├── client/        # Java Client tests
│   │           └── persistence/   # Java Client Persistence tests
│   └── build.sbt
│
├── macros/                        # SQL validation module
│   └──  src/
│      └── main/scala/
│          └── app/softnetwork/elastic/sq/macros
│
├── macros-tests/                  # SQL validation tests
│   └──  test/
│      └── main/scala/
│          └── app/softnetwork/elastic/sq/macros
│
├── persistence/                   # Akka persistence integration module
│   ├── src/
│   │   ├── main/scala/
│   │   │   └── app/softnetwork/elastic/persistence/
│   │   │       ├── query/        # Event Processor Stream
│   │   │       └── typed/        
│   │   └── test/scala/
│   │       └── app/softnetwork/elastic/
│   │           └── client/        # Unit tests
│   └── build.sbt
│
├── project/                       # sbt build configuration
│   ├── build.properties
│   ├── plugins.sbt
│   ├── SoftClient4es.scala
│   └── Versions.scala
│
├── sql/                           # SQL translation module
│   ├── src/
│   │   ├── main/scala/
│   │   │   └── app/softnetwork/elastic/sql/
│   │   └── test/scala/
│   │       └── app/softnetwork/elastic/sql/
│   └── build.sbt
│
├── testkit/                       # Testing utilities (Docker integration, test specs)
│   ├── src/
│   │   └── main/scala/
│   │       └── app/softnetwork/elastic/
│   │           ├── client/        # Client specifications for tests
│   │           ├── model/         # Entities for persistence tests
│   │           ├── persistence/   # Persistence specifications for tests
│   │           └── scalatest/     # Testkit utilities
│   └── build.sbt
│
├── .scalafmt.conf                 # Scalafmt configuration
├── .gitignore
├── build.sbt                      # Root build configuration
├── README.md
├── CONTRIBUTING.md
├── LICENSE
└── CHANGELOG.md
```

### **Module Descriptions**

| Module        | Purpose                                     | Dependencies                      |
|---------------|---------------------------------------------|-----------------------------------|
| `sql`         | SQL Parser                                  | Gson                              |
| `bridge`      | SQL to Elasticsearch DSL translation        | sql, Elastic4s                    |
| `macros`      | SQL Query type-safe validation              | sql                               |
| `core`        | Core abstractions, interfaces, and models   | macros, Akka Streams, config      |
| `persistence` | Akka persistence integration                | core, Akka persistence            |
| `jest`        | Elasticsearch 6.x client implementation     | core, Jest client                 |
| `rest`        | Elasticsearch 7.x client implementation     | core, REST High-Level client      |
| `java`        | Elasticsearch 8.x/9.x client implementation | core, Java API client             |
| `testkit`     | Testing utilities and Docker integration    | core, persistence, Testcontainers |

---

## **Development Workflow**

### **Branch Naming Convention**

Use descriptive branch names following this pattern:

```
<type>/<short-description>

Types:
- feature/  : New features
- fix/      : Bug fixes
- docs/     : Documentation changes
- refactor/ : Code refactoring
- test/     : Test improvements
- chore/    : Maintenance tasks
```

**Examples:**
```
feature/add-point-in-time-api
fix/bulk-operation-memory-leak
docs/update-migration-guide
refactor/simplify-error-handling
test/improve-scroll-api-coverage
chore/upgrade-elasticsearch-dependencies
```

### **Creating a Branch**

```bash
# Update your fork
git checkout main
git fetch upstream
git merge upstream/main

# Create a new branch
git checkout -b feature/my-new-feature

# Push branch to your fork
git push -u origin feature/my-new-feature
```

### **Making Changes**

```bash
# Make your changes
# ...

# Check code formatting
sbt scalafmtCheck

# Format code if needed
sbt scalafmtAll

# Run tests
sbt test

# Run integration tests
sbt it:test

# Stage changes
git add .

# Commit with descriptive message
git commit -m "feat: add Point-in-Time API support for ES 8.x

- Implement openPointInTime and closePointInTime methods
- Add PIT support to search API
- Add comprehensive tests
- Update documentation

Closes #123"
```

### **Commit Message Guidelines**

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `perf`: Performance improvements

**Examples:**

```bash
# Feature
git commit -m "feat(java): add Point-in-Time API support

Implement PIT API for Elasticsearch 8.x and 9.x with automatic
resource cleanup and comprehensive error handling.

Closes #123"

# Bug fix
git commit -m "fix(bulk): resolve memory leak in bulk operations

Fix memory leak caused by unclosed scroll contexts in bulk
streaming operations.

Fixes #456"

# Documentation
git commit -m "docs(best-practices): add section on connection pooling

Add detailed guidance on configuring connection pools for
different workload patterns."

# Breaking change
git commit -m "feat(core): change ElasticResult API to use Either

BREAKING CHANGE: ElasticResult now uses Either[ElasticError, T]
instead of custom success/failure types. Update pattern matching
accordingly.

Migration guide: documentation/migration.md

Closes #789"
```

### **Keeping Your Branch Updated**

```
# Fetch latest changes from upstream
git fetch upstream

# Rebase your branch on upstream/main
git rebase upstream/main

# If conflicts occur, resolve them and continue
git add .
git rebase --continue

# Force push to your fork (only if you've already pushed)
git push --force-with-lease origin feature/my-new-feature
```

---

## **Coding Standards**

### **Scala Style Guide**

We follow the [Scala Style Guide](https://docs.scala-lang.org/style/) with some project-specific conventions.

#### **Formatting**

✅ **DO**: Use ScalafmtAll for automatic formatting

```bash
# Format all files
sbt scalafmtAll

# Check formatting
sbt scalafmtCheck
```

Our `.scalafmt.conf`:
```hocon
version=3.0.2
style = defaultWithAlign
align.openParenCallSite = false
align.openParenDefnSite = false
align.tokens = [{code = "->"}, {code = "<-"}, {code = "=>", owner = "Case"}]
continuationIndent.callSite = 2
continuationIndent.defnSite = 2
danglingParentheses = true
indentOperator = spray
maxColumn = 100
indentOperator.preset = "spray"
danglingParentheses.preset = true
project.excludeFilters = [".*\\.sbt"]
rewrite.rules = [RedundantParens, SortImports]
spaces.inImportCurlyBraces = false
unindentTopLevelOperators = true
project.git = true
```

#### **Naming Conventions**

```scala
// Classes and traits: PascalCase
class ElasticClientApi
trait SearchApi

// Objects: PascalCase
object ElasticClientFactory

// Methods and values: camelCase
def createIndex(name: String): ElasticResult[Boolean]
val maxConnections: Int = 100

// Constants: camelCase (not SCREAMING_SNAKE_CASE)
val defaultTimeout: FiniteDuration = 30.seconds

// Type parameters: Single uppercase letter or PascalCase
def search[T](query: String): ElasticResult[T]
def map[Result](f: T => Result): ElasticResult[Result]

// Package names: lowercase
package app.softnetwork.elastic.client
```

#### **Code Organization**

✅ **DO**: Use meaningful variable names

```scala
// Good
val indexName: String = "users"
val searchResults: ElasticResult[SearchResponse] = client.search(indexName, query)

// Bad
val x: String = "users"
val r: ElasticResult[SearchResponse] = client.search(x, q)
```

#### **Type Annotations**

✅ **DO**: Add type annotations for public APIs

```scala
// Good: Public API with explicit return type
def search(index: String, query: String): ElasticResult[SearchResponse] = {
  // Implementation
}

// Good: Private method can infer type
private def buildQuery(term: String) = {
  s"""{"query":{"match":{"name":"$term"}}}"""
}

// Good: Complex types should be annotated
val config: ElasticConfig = ElasticConfig(
  credentials = ElasticCredentials("http://localhost:9200")
)
```

#### **Pattern Matching**

✅ **DO**: Use exhaustive pattern matching

```scala
// Good: Exhaustive matching
result match {
  case ElasticSuccess(response) =>
    logger.info(s"Success: ${response.id}")
  case ElasticFailure(error) =>
    logger.error(s"Failure: ${error.message}")
}

// Good: Use @switch annotation for performance
(status: @switch) match {
  case 200 => "OK"
  case 404 => "Not Found"
  case 500 => "Server Error"
  case _ => "Unknown"
}
```

#### **Documentation**

✅ **DO**: Add Scaladoc for public APIs

```scala
/**
 * Searches for documents in the specified index.
 *
 * @param index the index name to search
 * @param query the Elasticsearch query DSL as JSON string
 * @param from the starting offset for pagination (default: 0)
 * @param size the number of results to return (default: 10)
 * @return ElasticSuccess with SearchResponse or ElasticFailure with error details
 *
 * @example
 * {{{
 * val query = """{"query":{"match":{"name":"John"}}}"""
 * client.search("users", query, from = 0, size = 20) match {
 *   case ElasticSuccess(response) =>
 *     response.hits.foreach(hit => println(hit.source))
 *   case ElasticFailure(error) =>
 *     logger.error(s"Search failed: ${error.message}")
 * }
 * }}}
 */
def search(
  index: String,
  query: String,
  from: Int = 0,
  size: Int = 10
): ElasticResult[SearchResponse]
```

#### **Immutability**

✅ **DO**: Prefer immutable data structures

```scala
// Good: Immutable case class
case class ElasticConfig(
  credentials: ElasticCredentials = ElasticCredentials(),
  multithreaded: Boolean = true,
  discovery: DiscoveryConfig,
  connectionTimeout: Duration,
  socketTimeout: Duration,
  metrics: MetricsConfig
)

// Good: Immutable collections
val indices: Seq[String] = Seq("users", "products", "orders")

// Bad: Mutable collections
val indices: mutable.Buffer[String] = mutable.Buffer("users")
```

#### **Function Composition**

✅ **DO**: Use functional composition

```scala
// Good: Functional composition
def validateAndIndex(index: String, document: String): ElasticResult[IndexResponse] = {
  for {
    validatedIndex <- validateIndexName(index)
    validatedDoc <- validateJson(document)
    response <- client.index(validatedIndex, validatedDoc)
  } yield response
}

// Good: Map over results
client.search("users", query)
  .map(_.hits)
  .map(_.map(_.source))
  .map(_.map(parseUser))
```

---

## **Testing Guidelines**

### **Test Structure**

We use **ScalaTest** with the **AnyFlatSpecLike** style.

```scala
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class ElasticClientSpec extends AnyFlatSpecLike with Matchers {
  
  "ElasticClient" should {
    
    "index documents successfully" in {
      val client = createTestClient()
      val document = """{"name":"Alice","age":30}"""
      
      val result = client.index("users", "user-0", document)
      
      result.isSuccess shouldBe true
    }
    
    "handle indexing errors gracefully" in {
      val client = createTestClient()
      val invalidDocument = """invalid json"""
      
      val result = client.index("users", invalidDocument)
      
      result.isFailure shouldBe true
      result.failed.get.message should include("JSON")
    }
  }
}
```

### **Unit Tests**

✅ **DO**: Write unit tests for business logic

```scala
class SearchQueryBuilderSpec extends AnyWordSpec with Matchers {
  
  "SearchQueryBuilder" should {
    
    "build match query correctly" in {
      val query = SearchQueryBuilder.matchQuery("name", "Alice")
      
      query should include(""""match"""")
      query should include(""""name"""")
      query should include(""""Alice"""")
    }
    
    "build range query correctly" in {
      val query = SearchQueryBuilder.rangeQuery("age", gte = Some(18), lte = Some(65))
      
      query should include(""""range"""")
      query should include(""""gte":18""")
      query should include(""""lte":65""")
    }
  }
}
```

### **Integration Tests**

✅ **DO**: Use `ElasticDockerTestKit` for integration tests

```scala
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class ElasticClientIntegrationSpec 
  extends AnyWordSpec 
  with ElasticDockerTestKit 
  with Matchers {
  
  "ElasticClient integration" should {
    
    "perform full CRUD operations" in {
      // Create index
      val mapping = """{
        "properties": {
          "name": {"type": "text"},
          "email": {"type": "keyword"}
        }
      }"""
      
      client.createIndex("users", mapping = Some(mapping))
      
      // Index document
      val docId = "user-0"
      val document = """{"name":"Alice","email":"alice@example.com"}"""
      val indexResult = client.index("users", docId, document)
      indexResult.isSuccess shouldBe true
      
      // Refresh
      client.refresh("users")
      
      // Get document
      val getResult = client.get("users", docId)
      getResult.isSuccess shouldBe true
      getResult.get.source should include("Alice")
      
      // Update document
      val update = """{"doc":{"name":"Alice Smith"}}"""
      client.update("users", docId, update)
      
      // Delete document
      val deleteResult = client.delete("users", docId)
      deleteResult.isSuccess shouldBe true
      
      // Cleanup
      client.deleteIndex("users")
    }
  }
}
```

### **Test Coverage**

✅ **DO**: Maintain high test coverage

```bash
# Generate coverage report
sbt clean coverage test coverageReport

# View report
open target/scala-2.13/scoverage-report/index.html
```

**Coverage Goals:**
- Core API: > 90%
- Client implementations: > 85%
- Utility functions: > 80%
- Overall project: > 85%

### **Test Naming**

✅ **DO**: Use descriptive test names

```scala
// Good: Clear what is being tested
"index documents with auto-generated IDs" in { ... }
"handle connection timeout errors" in { ... }
"validate index names before creation" in { ... }

// Bad: Unclear test purpose
"test1" in { ... }
"it works" in { ... }
```

### **Test Data**

✅ **DO**: Use realistic test data

```scala
object TestData {
  val validUser: String = """{
    "name": "Alice Smith",
    "email": "alice@example.com",
    "age": 30,
    "created_at": "2024-01-15T10:30:00Z"
  }"""
  
  val validProduct: String = """{
    "name": "Laptop",
    "price": 999.99,
    "category": "electronics",
    "in_stock": true
  }"""
  
  val invalidJson: String = """{"name": "Alice", "age": }"""
}
```

### **Async Testing**

✅ **DO**: Use ScalaTest's async support

```scala
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._

class AsyncClientSpec 
  extends AnyWordSpec 
  with Matchers 
  with ScalaFutures {
  
  implicit val patience: PatienceConfig = PatienceConfig(
    timeout = 10.seconds,
    interval = 100.millis
  )
  
  "AsyncClient" should {
    
    "index documents asynchronously" in {
      val future = asyncClient.indexAsync("users", document)
      
      whenReady(future) { result =>
        result.isSuccess shouldBe true
        result.get.id should not be empty
      }
    }
  }
}
```

---

## **Pull Request Process**

### **Before Submitting**

✅ **Checklist:**
- [ ] Code compiles without errors
- [ ] All tests pass (`sbt test`)
- [ ] Code is formatted (`sbt scalafmtAll`)
- [ ] No scalafmt violations (`sbt scalafmtCheck`)
- [ ] Test coverage is maintained or improved
- [ ] Documentation is updated
- [ ] CHANGELOG.md is updated
- [ ] Commit messages follow conventions
- [ ] Branch is rebased on latest `main`

### **Submitting a Pull Request**

1. **Push your branch to your fork**

```
git push origin feature/my-new-feature
```

2. **Create Pull Request on GitHub**

- Go to your fork on GitHub
- Click "New Pull Request"
- Select your feature branch
- Fill out the PR template

### **Pull Request Template**

```
## Description

Brief description of the changes in this PR.

## Type of Change

- [ ] Bug fix (non-breaking change fixing an issue)
- [ ] New feature (non-breaking change adding functionality)
- [ ] Breaking change (fix or feature causing existing functionality to change)
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring

## Related Issues

Closes #123
Fixes #456

## Changes Made

- Added Point-in-Time API support for ES 8.x
- Implemented automatic resource cleanup
- Added comprehensive tests
- Updated documentation

## Testing

Describe the tests you ran and how to reproduce them.

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed

### Test Environment

- Elasticsearch version: 8.11.0
- Scala version: 2.13.12
- JDK version: 11

## Documentation

- [ ] README updated
- [ ] API documentation updated
- [ ] Migration guide updated (if breaking change)
- [ ] CHANGELOG updated

## Screenshots (if applicable)

Add screenshots for UI changes or visual documentation.

## Checklist

- [ ] Code compiles without errors
- [ ] All tests pass
- [ ] Code is formatted with scalafmt
- [ ] Test coverage maintained/improved
- [ ] Documentation updated
- [ ] Commit messages follow conventions
- [ ] Branch rebased on latest main

## Additional Notes

Any additional information reviewers should know.
```

### **Pull Request Review Process**

1. **Automated Checks**: CI/CD pipeline runs automatically
  - Compilation
  - Unit tests
  - Code formatting
  - Coverage report

2. **Code Review**: Maintainers review your code
  - Code quality
  - Test coverage
  - Documentation
  - Design decisions

3. **Feedback**: Address review comments
  - Make requested changes
  - Push updates to your branch
  - Respond to comments

4. **Approval**: Once approved, maintainers will merge

### **Addressing Review Comments**

```bash
# Make requested changes
# ...

# Commit changes
git add .
git commit -m "refactor: address review comments

- Simplify error handling logic
- Add missing test cases
- Update documentation"

# Push to your branch
git push origin feature/my-new-feature
```

### **After Merge**

```bash
# Update your local main branch
git checkout main
git fetch upstream
git merge upstream/main

# Delete your feature branch
git branch -d feature/my-new-feature
git push origin --delete feature/my-new-feature
```

---

## **Documentation**

### **Types of Documentation**

1. **Code Documentation**: Scaladoc comments in code
2. **API Documentation**: Detailed API reference
3. **User Guides**: How-to guides and tutorials
4. **Best Practices**: Recommended patterns
5. **Migration Guides**: Version upgrade instructions

### **Documentation Location**

- **API Reference**: `documentation/client/`
- **User Guides**: `documentation/guides/`
- **Best Practices**: `documentation/best-practices.md`
- **Migration Guides**: `documentation/migration.md`
- **Examples**: `examples/`

---

## **Release Process**

### **Versioning**

We follow [Semantic Versioning](https://semver.org/):

```
MAJOR.MINOR.PATCH

- MAJOR: Breaking changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes (backward compatible)
```

### **Release Workflow**

1. **Create Release Branch**

```bash
git checkout -b release/v0.12.0
```

2. **Update Version**

```scala
// build.sbt
version := "0.12.0"
```

3. **Update CHANGELOG**

```markdown
## [0.12.0] - 2025-11-08

### Added
- Point-in-Time API support for ES 8.x
- Automatic resource cleanup

### Changed
- Improved error messages

### Fixed
- Memory leak in bulk operations

### Breaking Changes
- None
```

4. **Create Release PR**

5. **After Merge, Tag Release**

```bash
git tag -a v0.12.0 -m "Release version 0.12.0"
git push upstream v0.12.0
```

6. **Publish Artifacts**

```
sbt +publish
```

---

## **Community**

### **Communication Channels**

- 💬 **GitHub Discussions**: [Discussions](https://github.com/SOFTNETWORK-APP/SoftClient4ES/discussions)
- 🐛 **GitHub Issues** : [Issues](https://github.com/SOFTNETWORK-APP/SoftClient4ES/issues)
- 📧 **Email**: admin@softnetwork.fr

[//]: # (- 🌐 **Website**: https://softnetwork.app)

### **Getting Help**

- **Search existing issues**: Your question might already be answered
- **Check documentation**: Review the docs before asking
- **Ask in Discussions**: For general questions and discussions
- **Open an issue**: For bugs or feature requests

### **Helping Others**

- Answer questions in Discussions
- Review pull requests
- Improve documentation
- Share your use cases and examples

### **Recognition**

We value all contributions! Contributors are recognized in:
- CONTRIBUTORS.md file
- Release notes
- Project README

---

## **Quick Reference**

### **Common Commands**

```bash
# Setup
git clone https://github.com/YOUR_USERNAME/softclient4es.git
cd softclient4es
sbt compile

# Development
sbt test                    # Run tests
sbt scalafmtAll             # Format code
sbt scalafmtCheck           # Check formatting

# Coverage
sbt clean coverage test coverageReport

# Create branch
git checkout -b feature/my-feature

# Commit
git add .
git commit -m "feat: add new feature"

# Push
git push origin feature/my-feature

# Update branch
git fetch upstream
git rebase upstream/main
git push --force-with-lease origin feature/my-feature
```

### **Resources**

- 📖 [Scala Style Guide](https://docs.scala-lang.org/style/)
- 📖 [Conventional Commits](https://www.conventionalcommits.org/)
- 📖 [Semantic Versioning](https://semver.org/)
- 📖 [ScalaTest Documentation](https://www.scalatest.org/)
- 📖 [sbt Documentation](https://www.scala-sbt.org/documentation.html)

---

## **Thank You!**

Thank you for contributing to SoftClient4ES! Your contributions help make this project better for everyone.

**Questions?** Feel free to ask in [GitHub Discussions](https://github.com/SOFTNETWORK-APP/SoftClient4ES/discussions) or email us at admin@softnetwork.fr.

---

**Last Updated:** 2025-11-12  
**Version:** 0.12.0

---

**Built with ❤️ by the SoftNetwork community**