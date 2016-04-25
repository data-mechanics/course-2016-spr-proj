#from datetime import datetime
import datetime
issue_date = "2015-02-25T00:00:00"
issue_time = "7:32:00 AM"
issue_datetime = issue_date.split('T')[0] + "T" + issue_time
print(issue_datetime)
d_format = '%Y-%m-%dT%I:%M:%S %p'
d = datetime.datetime.strptime(issue_datetime, d_format)
print(d.strftime(d_format))

issue_date = "2015-02-25T00:00:00"
issue_time = "7:32:00 PM"
issue_datetime = issue_date.split('T')[0] + "T" + issue_time
print(issue_datetime)
e = datetime.datetime.strptime(issue_datetime, d_format)
print(e.strftime(d_format))

issue_date = "2015-02-18T00:00:00"
issue_time = "7:05:00 PM"
issue_datetime = issue_date.split('T')[0] + "T" + issue_time
print(issue_datetime)
f = datetime.datetime.strptime(issue_datetime, d_format)
print(f.strftime(d_format))

issue_date = "2015-02-24T00:00:00"
issue_time = "7:05:00 PM"
issue_datetime = issue_date.split('T')[0] + "T" + issue_time
print(issue_datetime)
g = datetime.datetime.strptime(issue_datetime, d_format)
print(g.strftime(d_format))

daytimes = {}
for date in [d,e,f,g]:
	hour = date.hour
	day_week = date.strftime('%A')
	print(date.strftime(d_format))
	print(hour)
	print(day_week)
	if ((day_week + " " + str(hour)) not in daytimes):
		daytimes[day_week + " " + str(hour)] = 1
	else:
		daytimes[day_week + " " + str(hour)] += 1

print(daytimes)