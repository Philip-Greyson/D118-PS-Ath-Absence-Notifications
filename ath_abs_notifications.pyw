# import modules
import datetime
import os
from datetime import *

import oracledb
import pysftp

DB_UN = os.environ.get('POWERSCHOOL_READ_USER')  # username for read-only database user
DB_PW = os.environ.get('POWERSCHOOL_DB_PASSWORD')  # the password for the database account
DB_CS = os.environ.get('POWERSCHOOL_PROD_DB')  # the IP address, port, and database name to connect to

print(f"Username: {DB_UN} | Password: {DB_PW} | Server: {DB_CS}")  # debug so we can see where oracle is trying to connect to/with

SCHOOL_CODES = ['5']
TEACHER_ROLE_NAMES = ['Lead Teacher', 'Co-teacher']  # the role names of the teachers that we want to include in the emails. These are found in roledef.name
ATTENDANCE_CODES = ['AB', 'HA', 'UH', 'UN', 'MH']

if __name__ == '__main__':  # main file execution
    with open('ath_abs_notifs_log.txt', 'w') as log:  # open logging file
        startTime = datetime.now()
        today = datetime.now()
        todaysDate = datetime.now()
        todaysDate = todaysDate.replace(hour=0, minute=0, second=0, microsecond=0)  # get the date without any timecode as all daily attendance codes have just the date and no time
        startTime = startTime.strftime('%H:%M:%S')
        print(f'INFO: Execution started at {startTime}')
        print(f'INFO: Execution started at {startTime}', file=log)
        with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:  # create the connecton to the database
            try:
                with con.cursor() as cur:  # start an entry
                    for schoolCode in SCHOOL_CODES:
                        termid = None
                        cur.execute("SELECT id, firstday, lastday, schoolid, dcid FROM terms WHERE schoolid = :school AND isyearrec = 0 ORDER BY dcid DESC", school=schoolCode)  # get a list of terms for the school, filtering to not full years
                        terms = cur.fetchall()
                        for term in terms:  # go through every term
                            termStart = term[1]
                            termEnd = term[2]
                            #compare todays date to the start and end dates
                            if ((termStart < today) and (termEnd > today)):
                                termid = str(term[0])
                                termDCID = str(term[4])
                                print(f'INFO: Found good term: {termid} | {termDCID}')
                                print(f'INFO: Found good term: {termid} | {termDCID}', file=log)
                        # check to see if we found a valid term before we continue
                        if termid:
                            courseList = []  # make an empty list that will contain the course dicts inside it and student dict
                            courseDict = {}
                            # find all courses with ath (athletics) or act (activities) in the name
                            cur.execute("SELECT course_number, course_name FROM courses WHERE (instr(course_name, 'ATH-') > 0 OR instr(course_name, 'ACT-') > 0)")
                            courses = cur.fetchall()
                            for course in courses:
                                courseNum = course[0]
                                courseName = course[1]
                                # print(course)  # debug
                                # next find all students in the current course in the currend term
                                cur.execute('SELECT students.student_number, students.first_name, students.last_name, cc.sectionid FROM cc LEFT JOIN students ON cc.studentid = students.id WHERE cc.course_number = :course AND cc.termid = :term', course=courseNum, term=termid)
                                students = cur.fetchall()
                                sectionsDict = {}  # dict for each section that will contain section ID, student list, and teachers info
                                for student in students:
                                    # print(student)
                                    studentNum = str(int(student[0]))
                                    studentFirst = student[1]
                                    studentLast = student[2]
                                    studentName = f'{studentFirst} {studentLast}'
                                    sectionID = student[3]  # section ID for the section that student is enrolled in
                                    # print(f'DBUG: Starting student {studentNum} in section {sectionID}')  # debug
                                    if not sectionsDict.get(sectionID):  # the first time we get a new section, need to initialize its sub-dicts
                                        # print(f'DBUG: No entry exists in the dict for section ID {sectionID}, initializing it')  # debug
                                        sectionsDict.update({sectionID: {'Teachers': {}, 'Students': {}}})
                                    # print(f'DBUG: Current sectionsDict looks like: {sectionsDict}')  # debug
                                    studentDict = sectionsDict.get(sectionID).get('Students')  # get the current student list
                                    studentDict.update({studentNum: studentName})  # add the student to the list of students in the current section
                                    # print(f'DBUG: Curent teachers sub-dict: {sectionsDict.get(sectionID).get('Teachers')}')  # debug
                                    if not sectionsDict.get(sectionID).get('Teachers').keys():  # if we dont have any entries in the teachers list, we need to find them
                                        # print(f'DBUG: No teacher info found for section {sectionID}, finding them')  # debug
                                        teacherDict = {}
                                        # find the teachers and co-teachers for the section
                                        cur.execute('SELECT u.email_addr, u.lastfirst, rd.name FROM sectionteacher st LEFT JOIN roledef rd ON st.roleid = rd.id LEFT JOIN schoolstaff staff ON st.teacherid = staff.id LEFT JOIN users u ON staff.users_dcid = u.dcid WHERE st.sectionid = :section', section=sectionID)
                                        teachers = cur.fetchall()
                                        for teacher in teachers:
                                            if teacher[2] in TEACHER_ROLE_NAMES:  # only add them to the teacher dict if they are one of the roles we want to notify
                                                if not teacherDict.get(teacher[0]):  # if they dont already exist in the teacher dict
                                                    teacherDict.update({teacher[0]: teacher[1]})
                                    else:
                                        teacherDict = sectionsDict.get(sectionID).get('Teachers')
                                    sectionsDict.update({sectionID: {'Students': studentDict, 'Teachers': teacherDict}})  # do the update with the new student and teacher info (if applicble) to the sections dict
                                if sectionsDict:
                                    # print(sectionsDict)  # debug
                                    courseDict.update({courseName: sectionsDict})
                            # print(courseDict)  # debug
                            for activity in courseDict.keys():
                                print(f'INFO: Starting activity {activity}')
                                sections = courseDict.get(activity).keys()
                                for section in sections:
                                    absenceList = []  # create an empty list each section that will store which students have actuall had an absence
                                    toEmail = ''  # set the to email blank each new section
                                    print(f'INFO: Found section for activity {activity} with ID {section}')
                                    teacherEmails = courseDict.get(activity).get(section).get('Teachers').keys()
                                    # append all the emails into a single string that will be used in the "To" field for the email
                                    for email in teacherEmails:  
                                        if toEmail == '':
                                            toEmail = email
                                        else:
                                            toEmail += f', {email}'
                                    print(f'DBUG: Emails for section {section} in activity {activity} will be sent to "{toEmail}"')
                                    students = courseDict.get(activity).get(section).get('Students').items()  # get the student number and name pairings as tuples
                                    for studentNum, studentName in students:
                                        # print(f'DBUG: Looking for absences for student number {studentNum} - {studentName}')
                                        cur.execute('SELECT att.att_code FROM ps_attendance_daily att LEFT JOIN students s ON s.id = att.studentid WHERE s.student_number = :student AND att.schoolid = :school AND att.att_date = :today', student=studentNum, school=schoolCode, today=todaysDate)
                                        absences = cur.fetchall()
                                        for absence in absences:
                                            print(f'DBUG: Student {studentNum} has an absence with code {absence[0]}')  # debug
                                            if absence[0] in ATTENDANCE_CODES:  # if the attendance code matches one of the ones we are looking for
                                                absenceList.append(f'{studentNum} - {studentName}')
                                    if absenceList:  # if there are any absences
                                        print(f'INFO: Sending emails about absences for the following students: {absenceList}')

            except Exception as er:
                print(f'ERROR while finding absences: {er}')