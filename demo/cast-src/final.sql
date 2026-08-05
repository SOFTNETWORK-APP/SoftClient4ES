CREATE TABLE jdbc_join_emp (emp_id KEYWORD, name KEYWORD, dept_id INTEGER, salary INTEGER, PRIMARY KEY (emp_id));
CREATE TABLE jdbc_join_dept (dept_id INTEGER, dept_name KEYWORD, region KEYWORD, PRIMARY KEY (dept_id));
COPY INTO jdbc_join_emp FROM 'employees.json' FILE_FORMAT = 'JSON';
COPY INTO jdbc_join_dept FROM 'departments.json' FILE_FORMAT = 'JSON';
SELECT d.dept_name, COUNT(*) AS headcount, AVG(e.salary) AS avg_salary, MAX(e.salary) AS top_salary FROM jdbc_join_emp e JOIN jdbc_join_dept d ON e.dept_id = d.dept_id GROUP BY d.dept_name HAVING AVG(e.salary) > 75000 ORDER BY AVG(e.salary) DESC;
CREATE TABLE high_earner_report AS SELECT e.name, e.salary, d.dept_name FROM jdbc_join_emp e JOIN jdbc_join_dept d ON e.dept_id = d.dept_id WHERE e.salary > 90000;
SELECT h.name, h.salary, d.region FROM high_earner_report h JOIN jdbc_join_dept d ON h.dept_name = d.dept_name ORDER BY h.salary DESC;
