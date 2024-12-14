from __future__ import annotations

import typing as t
from typing import cast

from sqlglot import exp, parser, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _select_all(table: exp.Expression) -> t.Optional[exp.Select]:
    return exp.select("*").from_(table, copy=False) if table else None


class PRQL(Dialect):
    DPIPE_IS_STRING_CONCAT = False

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ["`"]
        QUOTES = ["'", '"']

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "=": TokenType.ALIAS,
            "'": TokenType.QUOTE,
            '"': TokenType.QUOTE,
            "`": TokenType.IDENTIFIER,
            "#": TokenType.COMMENT,
        }

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }

    class Parser(parser.Parser):
        CONJUNCTION = {
            **parser.Parser.CONJUNCTION,
            TokenType.DAMP: exp.And,
        }

        DISJUNCTION = {
            **parser.Parser.DISJUNCTION,
            TokenType.DPIPE: exp.Or,
        }

        TRANSFORM_PARSERS = {
            "DERIVE": lambda self, query: self._parse_selection(query),
            "SELECT": lambda self, query: self._parse_selection(query, append=False),
            "TAKE": lambda self, query: self._parse_take(query),
            "FILTER": lambda self, query: query.where(self._parse_assignment()),
            "APPEND": lambda self, query: query.union(
                _select_all(self._parse_table()), distinct=False, copy=False
            ),
            "REMOVE": lambda self, query: query.except_(
                _select_all(self._parse_table()), distinct=False, copy=False
            ),
            "INTERSECT": lambda self, query: query.intersect(
                _select_all(self._parse_table()), distinct=False, copy=False
            ),
            "SORT": lambda self, query: self._parse_order_by(query),
            "AGGREGATE": lambda self, query: self._parse_selection(
                query, parse_method=self._parse_aggregate, append=False
            ),
            "JOIN": lambda self, query: self._parse_join_prql(query),
        }

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "AVERAGE": exp.Avg.from_arg_list,
            "SUM": lambda args: exp.func("COALESCE", exp.Sum(this=seq_get(args, 0)), 0),
        }

        def _parse_equality(self) -> t.Optional[exp.Expression]:
            eq = self._parse_tokens(self._parse_comparison, self.EQUALITY)
            if not isinstance(eq, (exp.EQ, exp.NEQ)):
                return eq

            # https://prql-lang.org/book/reference/spec/null.html
            if isinstance(eq.expression, exp.Null):
                is_exp = exp.Is(this=eq.this, expression=eq.expression)
                return is_exp if isinstance(eq, exp.EQ) else exp.Not(this=is_exp)
            if isinstance(eq.this, exp.Null):
                is_exp = exp.Is(this=eq.expression, expression=eq.this)
                return is_exp if isinstance(eq, exp.EQ) else exp.Not(this=is_exp)
            return eq

        def _parse_statement(self) -> t.Optional[exp.Expression]:
            expression = self._parse_expression()
            expression = expression if expression else self._parse_query()
            return expression

        def _parse_query(self) -> t.Optional[exp.Query]:
            from_ = self._parse_from()

            if not from_:
                return None

            query = exp.select("*").from_(from_, copy=False)

            while self._match_texts(self.TRANSFORM_PARSERS):
                query = self.TRANSFORM_PARSERS[self._prev.text.upper()](self, query)

            return query

        def _parse_selection(
            self,
            query: exp.Query,
            parse_method: t.Optional[t.Callable] = None,
            append: bool = True,
        ) -> exp.Query:
            parse_method = parse_method if parse_method else self._parse_expression
            if self._match(TokenType.L_BRACE):
                selects = self._parse_csv(parse_method)

                if not self._match(TokenType.R_BRACE, expression=query):
                    self.raise_error("Expecting }")
            else:
                expression = parse_method()
                selects = [expression] if expression else []

            projections = {
                select.alias_or_name: select.this if isinstance(select, exp.Alias) else select
                for select in query.selects
            }

            selects = [
                select.transform(
                    lambda s: (projections[s.name].copy() if s.name in projections else s)
                    if isinstance(s, exp.Column)
                    else s,
                    copy=False,
                )
                for select in selects
            ]

            return query.select(*selects, append=append, copy=False)

        def _parse_take(self, query: exp.Query) -> t.Optional[exp.Query]:
            num = self._parse_number()  # TODO: TAKE for ranges a..b
            return query.limit(num) if num else None

        def _parse_ordered(
            self, parse_method: t.Optional[t.Callable] = None
        ) -> t.Optional[exp.Ordered]:
            asc = self._match(TokenType.PLUS)
            desc = self._match(TokenType.DASH) or (asc and False)
            term = term = super()._parse_ordered(parse_method=parse_method)
            if term and desc:
                term.set("desc", True)
                term.set("nulls_first", False)
            return term

        def _parse_order_by(self, query: exp.Select) -> t.Optional[exp.Query]:
            l_brace = self._match(TokenType.L_BRACE)
            expressions = self._parse_csv(self._parse_ordered)
            if l_brace and not self._match(TokenType.R_BRACE):
                self.raise_error("Expecting }")
            return query.order_by(self.expression(exp.Order, expressions=expressions), copy=False)

        def _parse_aggregate(self) -> t.Optional[exp.Expression]:
            alias = None
            if self._next and self._next.token_type == TokenType.ALIAS:
                alias = self._parse_id_var(any_token=True)
                self._match(TokenType.ALIAS)

            name = self._curr and self._curr.text.upper()
            func_builder = self.FUNCTIONS.get(name)
            if func_builder:
                self._advance()
                args = self._parse_column()
                func = func_builder([args])
            else:
                self.raise_error(f"Unsupported aggregation function {name}")
            if alias:
                return self.expression(exp.Alias, this=func, alias=alias)
            return func

        def _parse_expression(self) -> t.Optional[exp.Expression]:
            if self._next and self._next.token_type == TokenType.ALIAS:
                alias = self._parse_id_var(True)
                self._match(TokenType.ALIAS)
                return self.expression(exp.Alias, this=self._parse_assignment(), alias=alias)
            return self._parse_assignment()

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
            is_db_reference: bool = False,
            parse_partition: bool = False,
        ) -> t.Optional[exp.Expression]:
            return self._parse_table_parts()

        def _parse_from(
            self, joins: bool = False, skip_from_token: bool = False
        ) -> t.Optional[exp.From]:
            if not skip_from_token and not self._match(TokenType.FROM):
                return None

            return self.expression(
                exp.From, comments=self._prev_comments, this=self._parse_table(joins=joins)
            )

        def _parse_join_prql(self, query: exp.Select) -> exp.Select:
            join_type = "INNER"

            side = self._get_join_type()
            if side:
                join_type = side

            alias, table_name = self._parse_table_and_alias()

            table = self.expression(
                exp.Table,
                this=exp.to_identifier(table_name),
                alias=exp.to_identifier(alias) if alias else None,
            )

            join_condition = self._parse_join_condition(query, alias, table)

            if join_condition:
                from_expr = cast(exp.From, query.args.get("from"))
                left_table = cast(exp.Expression, from_expr.args["this"]).alias_or_name
                right_table = alias or table.alias_or_name
                join_condition = self._replace_this_that(join_condition, left_table, right_table)

            join = exp.Join(this=table, kind=join_type, on=join_condition)
            query = query.join(join)
            return query

        def _parse_table_and_alias(self) -> t.Tuple[str | None, str]:
            table_name = cast(str, self._parse_id_var())
            alias = None

            if self._match(TokenType.ALIAS):
                alias = table_name
                table_name = cast(str, self._parse_id_var())

            return alias, table_name

        def _get_join_type(self) -> t.Optional[str]:
            if self._match_texts({"SIDE"}):
                if self._match(TokenType.COLON):
                    if self._match(TokenType.LEFT):
                        return "LEFT"
                    elif self._match(TokenType.RIGHT):
                        return "RIGHT"
                    elif self._match(TokenType.FULL):
                        return "FULL"
                    elif self._match(TokenType.INNER):
                        return "INNER"
                    else:
                        self.raise_error(
                            "Unsupported join side. Expected LEFT, RIGHT, FULL, or INNER."
                        )
            return None

        def _parse_join_condition(
            self, query: exp.Select, alias: str | None, table: exp.Table
        ) -> t.Optional[exp.Expression]:
            join_condition: t.Optional[exp.Expression] = None

            if self._match(TokenType.TRUE):
                join_condition = self.expression(exp.Boolean, this=True)

            elif self._match(TokenType.L_PAREN):
                if self._match(TokenType.EQ):
                    column_name = cast(str, self._parse_id_var())
                    if column_name:
                        from_expr = cast(exp.From, query.args.get("from"))
                        left_table = cast(exp.Expression, from_expr.args["this"]).alias_or_name
                        right_table = alias or table.alias_or_name
                        join_condition = self.expression(
                            exp.EQ,
                            this=exp.Column(
                                this=exp.to_identifier(column_name),
                                table=exp.to_identifier(left_table),
                            ),
                            expression=exp.Column(
                                this=exp.to_identifier(column_name),
                                table=exp.to_identifier(right_table),
                            ),
                        )
                else:
                    join_condition = self._parse_conjunction()

                if not self._match(TokenType.R_PAREN):
                    self.raise_error("Expected ')' at the end of join condition")

            return join_condition

        def _replace_this_that(
            self, condition: exp.Expression, left_table: str, right_table: str
        ) -> exp.Expression:
            replacement_map = {
                "this": left_table,
                "that": right_table,
            }

            def transform(expression: exp.Expression) -> exp.Expression:
                if isinstance(expression, exp.Column) and expression.table:
                    lower_table = expression.table.lower()
                    if lower_table in replacement_map:
                        expression.set("table", replacement_map[lower_table])
                return expression

            return condition.transform(transform)
