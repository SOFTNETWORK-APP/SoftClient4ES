package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{Delete, Insert, Update}

trait DmlParser { self: Parser with WhereParser => }
