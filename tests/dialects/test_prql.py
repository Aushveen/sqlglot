from tests.dialects.test_dialect import Validator


class TestPRQL(Validator):
    dialect = "prql"

    def test_prql(self):
        self.validate_all(
            "from x",
            write={
                "": "SELECT * FROM x",
            },
        )
        self.validate_all(
            "from x derive a + 1",
            write={
                "": "SELECT *, a + 1 FROM x",
            },
        )
        self.validate_all(
            "from x derive x = a + 1",
            write={
                "": "SELECT *, a + 1 AS x FROM x",
            },
        )
        self.validate_all(
            "from x derive {a + 1}",
            write={
                "": "SELECT *, a + 1 FROM x",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1, b}",
            write={
                "": "SELECT *, a + 1 AS x, b FROM x",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1, b} select {y = x, 2}",
            write={"": "SELECT a + 1 AS y, 2 FROM x"},
        )
        self.validate_all(
            "from x take 10",
            write={
                "": "SELECT * FROM x LIMIT 10",
            },
        )
        self.validate_all(
            "from x take 10 take 5",
            write={
                "": "SELECT * FROM x LIMIT 5",
            },
        )
        self.validate_all(
            "from x filter age > 25",
            write={
                "": "SELECT * FROM x WHERE age > 25",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1, b} filter age > 25",
            write={
                "": "SELECT *, a + 1 AS x, b FROM x WHERE age > 25",
            },
        )
        self.validate_all(
            "from x filter dept != 'IT'",
            write={
                "": "SELECT * FROM x WHERE dept <> 'IT'",
            },
        )
        self.validate_all(
            "from x filter p == 'product' select { a, b }",
            write={"": "SELECT a, b FROM x WHERE p = 'product'"},
        )
        self.validate_all(
            "from x filter age > 25 filter age < 27",
            write={"": "SELECT * FROM x WHERE age > 25 AND age < 27"},
        )
        self.validate_all(
            "from x filter (age > 25 && age < 27)",
            write={"": "SELECT * FROM x WHERE (age > 25 AND age < 27)"},
        )
        self.validate_all(
            "from x filter (age > 25 || age < 27)",
            write={"": "SELECT * FROM x WHERE (age > 25 OR age < 27)"},
        )
        self.validate_all(
            "from x filter (age > 25 || age < 22) filter age > 26 filter age < 27",
            write={
                "": "SELECT * FROM x WHERE ((age > 25 OR age < 22) AND age > 26) AND age < 27",
            },
        )
        self.validate_all(
            "from x sort age",
            write={
                "": "SELECT * FROM x ORDER BY age",
            },
        )
        self.validate_all(
            "from x sort {-age}",
            write={
                "": "SELECT * FROM x ORDER BY age DESC",
            },
        )
        self.validate_all(
            "from x sort {age, name}",
            write={
                "": "SELECT * FROM x ORDER BY age, name",
            },
        )
        self.validate_all(
            "from x sort {-age, +name}",
            write={
                "": "SELECT * FROM x ORDER BY age DESC, name",
            },
        )
        self.validate_all(
            "from x append y",
            write={
                "": "SELECT * FROM x UNION ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x remove y",
            write={
                "": "SELECT * FROM x EXCEPT ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x intersect y",
            write={"": "SELECT * FROM x INTERSECT ALL SELECT * FROM y"},
        )
        self.validate_all(
            "from x filter a == null filter null != b",
            write={
                "": "SELECT * FROM x WHERE a IS NULL AND NOT b IS NULL",
            },
        )
        self.validate_all(
            "from x filter (a > 1 || null != b || c != null)",
            write={
                "": "SELECT * FROM x WHERE (a > 1 OR NOT b IS NULL OR NOT c IS NULL)",
            },
        )
        self.validate_all(
            "from a aggregate { average x }",
            write={
                "": "SELECT AVG(x) FROM a",
            },
        )
        self.validate_all(
            "from a aggregate { average x, min y, ct = sum z }",
            write={
                "": "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) AS ct FROM a",
            },
        )
        self.validate_all(
            "from a aggregate { average x, min y, sum z }",
            write={
                "": "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) FROM a",
            },
        )
        self.validate_all(
            "from a aggregate { min y, b = stddev x, max z }",
            write={
                "": "SELECT MIN(y), STDDEV(x) AS b, MAX(z) FROM a",
            },
        )
        self.validate_all(
            "from tracks join side:left artists (artists.id==tracks.artist_id && artists.country=='UK')",
            write={
                "": "SELECT * FROM tracks LEFT JOIN artists ON artists.id = tracks.artist_id AND artists.country = 'UK'"
            },
        )
        self.validate_all(
            "from employees join side:left p=positions (employees.id==p.employee_id)",
            write={
                "": "SELECT * FROM employees LEFT JOIN positions AS p ON employees.id = p.employee_id"
            },
        )
        self.validate_all(
            "from employees join side:full departments (employees.dept_id==departments.id)",
            write={
                "": "SELECT * FROM employees FULL JOIN departments ON employees.dept_id = departments.id",
            },
        )
        self.validate_all(
            "from employees join side:right positions (employees.id==positions.employee_id)",
            write={
                "": "SELECT * FROM employees RIGHT JOIN positions ON employees.id = positions.employee_id",
            },
        )
        self.validate_all(
            "from shirts join hats true", write={"": "SELECT * FROM shirts INNER JOIN hats ON TRUE"}
        )
        self.validate_all(
            "from tracks join side:inner artists (this.id==that.artist_id)",
            write={"": "SELECT * FROM tracks INNER JOIN artists ON tracks.id = artists.artist_id"},
        )
        self.validate_all(
            "from employees join e=employees (employees.manager_id==e.id)",
            write={
                "": "SELECT * FROM employees INNER JOIN employees AS e ON employees.manager_id = e.id",
            },
        )
        self.validate_all(
            "from employees join positions (==emp_no)",
            write={
                "": "SELECT * FROM employees INNER JOIN positions ON employees.emp_no = positions.emp_no"
            },
        )
        self.validate_all(
            "from orders join customers (==customer_id) join products (orders.product_id==products.id)",
            write={
                "": "SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.customer_id INNER JOIN products ON orders.product_id = products.id",
            },
        )
        self.validate_all(
            "from employees join side:left positions (employees.id==positions.employee_id && (positions.title=='Manager' || positions.level>5))",
            write={
                "": "SELECT * FROM employees LEFT JOIN positions ON employees.id = positions.employee_id AND (positions.title = 'Manager' OR positions.level > 5)",
            },
        )
        self.validate_all(
            "from sales join products (products.id == sales.product_id && products.category == 'electronics')",
            write={
                "": "SELECT * FROM sales INNER JOIN products ON products.id = sales.product_id AND products.category = 'electronics'",
            },
        )
        self.validate_all(
            "from employees join side:full positions (employees.id==positions.employee_id && positions.department_id!=employees.dept_id)",
            write={
                "": "SELECT * FROM employees FULL JOIN positions ON employees.id = positions.employee_id AND positions.department_id <> employees.dept_id",
            },
        )
        self.validate_all(
            "from employees join side:inner departments (==dept_id) join side:left m=managers (employees.manager_id==m.id) join side:inner projects (employees.project_id==projects.id && projects.status=='active')",
            write={
                "": "SELECT * FROM employees INNER JOIN departments ON employees.dept_id = departments.dept_id LEFT JOIN managers AS m ON employees.manager_id = m.id INNER JOIN projects ON employees.project_id = projects.id AND projects.status = 'active'",
            },
        )
        self.validate_all(
            "from orders join o=orders (this.customer_id==that.customer_id && that.id!=this.id)",
            write={
                "": "SELECT * FROM orders INNER JOIN orders AS o ON orders.customer_id = o.customer_id AND o.id <> orders.id",
            },
        )
        self.validate_all(
            "from shirts join hats true join s=shoes true",
            write={
                "": "SELECT * FROM shirts INNER JOIN hats ON TRUE INNER JOIN shoes AS s ON TRUE",
            },
        )
        self.validate_all(
            "from orders join side:left customers (orders.customer_id==customers.id) join side:inner products (orders.product_id==products.id)",
            write={
                "": "SELECT * FROM orders LEFT JOIN customers ON orders.customer_id = customers.id INNER JOIN products ON orders.product_id = products.id",
            },
        )
        self.validate_all(
            "from employees join departments (employees.dept_id==departments.id) filter (employees.tenure > 10)",
            write={
                "": "SELECT * FROM employees INNER JOIN departments ON employees.dept_id = departments.id WHERE (employees.tenure > 10)",
            },
        )
        self.validate_all(
            "from employees join departments (employees.dept_id==departments.id && (departments.location=='HQ' || departments.budget > 100000))",
            write={
                "": "SELECT * FROM employees INNER JOIN departments ON employees.dept_id = departments.id AND (departments.location = 'HQ' OR departments.budget > 100000)"
            },
        )
        self.validate_all(
            "from employees join manager=employees (employees.manager_id==manager.id) join director=employees (employees.director_id==director.id)",
            write={
                "": "SELECT * FROM employees INNER JOIN employees AS manager ON employees.manager_id = manager.id INNER JOIN employees AS director ON employees.director_id = director.id"
            },
        )
        self.validate_all(
            "from shirts join hats true join side:left shoes true join side:right bags (shirts.id==bags.id) join accessories true filter shirts.in_stock == true && hats.is_available == true",
            write={
                "": "SELECT * FROM shirts INNER JOIN hats ON TRUE LEFT JOIN shoes ON TRUE RIGHT JOIN bags ON shirts.id = bags.id INNER JOIN accessories ON TRUE WHERE shirts.in_stock = TRUE AND hats.is_available = TRUE"
            },
        )
