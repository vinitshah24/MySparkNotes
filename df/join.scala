val employee = Seq(
	(1,"ramu", 3,"2018", 10001, "M", 25000),
	(2,"raju", 1,"2010", 20001, "M", 40000),
	(3,"mahesh", 1,"2010", 10001, "M", 35000),
	(4,"suresh", 2,"2005", 10001, "M", 45000),
	(5,"likitha", 2,"2010", 40001, "F", 35000),
	(6,"lavanya", 2,"2010", 50001, "F", 40000),
	(8,"madhu", 1,"2011", 50001,"", 40000)
)
val empSchema = Seq("emp_id", "name", "head_id", "year_joined", "dept_id", "gender", "salary")
val employeeDF = employee.toDF(empSchema:_*)

val dept = Seq(
	("Accounts", 10001),
	("Marketing", 20001),
	("Finance", 30001),
	("Engineering", 40001)
)
val deptSchema = Seq("department", "dept_id")
val deptDf = dept.toDF(deptSchema:_*)

val innerDf = employeeDF.join(deptDf, employeeDF("dept_id") === deptDf("dept_id"), "inner")
innerDf.show(false)
