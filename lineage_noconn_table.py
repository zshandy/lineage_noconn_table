from sqlglot import parse_one, exp
import os
import re
import json
from typing import List

rem_regex = re.compile(r"[^a-zA-Z0-9_]")


class ColumnLineageNoConn:
    def __init__(self, sql_list: List[str] = None):
        self.output_dict = {}
        self.json_dict = {}
        self.insertion_dict = {}
        self.deletion_dict = {}
        self.analyze_list = []
        self.curr_name = ""
        self.sql_list = sql_list
        self.sql_ast = None
        self.get_file_name()
        self.table_alias_dict = {}
        self.cte_table_dict = {}
        self.table_list = []
        #print(self.output_dict)
        #print(self.json_dict)

    def get_file_name(self):
        for sql in self.sql_list:
            sql = self._remove_comments(sql, self.sql_list.index(sql))
            self.sql_ast = parse_one(sql)
            self.table_alias_dict = {}
            self.cte_table_dict = {}
            self.table_list = []
            self.run_cte_lineage()
            # Everything other than CTEs, and pop the CTE tree
            for with_sql in self.sql_ast.find_all(exp.With):
                with_sql.pop()
            self.run_lineage(self.sql_ast)
            if not str(self.curr_name).endswith("_ANALYZED"):
                self.output_dict[self.curr_name] = self.table_list
        # To sub all the names that is after the ANALYZED query
        for name in self.analyze_list:
            for key, value in self.output_dict.copy().items():
                temp_value = []
                for w in value:
                    if w == name:
                        temp_value.append(w + '_ANALYZED')
                    else:
                        temp_value.append(w)
                self.output_dict[key] = temp_value
        # To transform into json format:
        for key, value in self.output_dict.items():
            self.json_dict[str(key)] = {
                "tables": value,
                "columns": {"dummycol": [s + ".dummycol" for s in value]},
                "table_name": str(key)
            }
        with open("output.json", "w") as outfile:
            json.dump(self.json_dict, outfile)
        self._produce_html(output_json=str(self.json_dict).replace("'", '"'))

    def run_lineage(self, sql_ast):
        main_tables = self.resolve_table(sql_ast)
        self.table_list = self.find_all_tables(main_tables)

    def run_cte_lineage(self):
        for cte in self.sql_ast.find_all(exp.CTE):
            temp_cte_table = self.resolve_table(cte)
            cte_name = cte.find(exp.TableAlias).alias_or_name
            self.cte_table_dict[cte_name] = list(set(self.find_all_tables(temp_cte_table)))

    def resolve_table(self, part_ast):
        temp_table_list = []
        for table in part_ast.find_all(exp.Table):
            temp_table_list = self.find_table(table, temp_table_list)
        return temp_table_list

    def find_table(self, table, temp_table_list):
        # Update table alias and find all aliased used table names
        if table.alias == "":
            self.table_alias_dict[table.sql()] = table.sql()
            temp_table_list.append(table.sql())
        else:
            temp = table.sql().split(" ")
            if temp[1] == "AS" or temp[1] == "as":
                self.table_alias_dict[temp[2]] = temp[0]
                temp_table_list.append(temp[0])
        return temp_table_list

    def find_all_tables(self, temp_table_list):
        # Update the used table names, such as if a CTE, update it with the dependant tables
        ret_table = []
        for i in temp_table_list:
            table_name = i
            if i in self.table_alias_dict.keys():
                table_name = self.table_alias_dict[i]
            if table_name in self.cte_table_dict.keys():
                ret_table.extend(self.cte_table_dict[table_name])
            else:
                ret_table.append(table_name)
        return ret_table

    def _remove_comments(self, input_str, idx):
        # remove the /* */ comments
        q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", input_str)
        # remove whole line -- and # comments
        lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
        # remove trailing -- and # comments
        q = " ".join([re.split("--|#", line)[0] for line in lines])
        # replace all spaces around commas
        q = re.sub(r'\s*,\s*', ',', q)
        # replace all multiple spaces to one space
        q = re.sub("\s\s+", " ", q)
        q = q.replace('`', '').strip()
        # adjust to INSERT/DELETE/SELECT/
        if q.find("INSERT INTO") != -1:
            # find the current name in the insertion dict and how many times it has been inserted
            self.curr_name = re.sub(rem_regex, "", q.split(" ")[2])
            if self.curr_name not in self.insertion_dict.keys():
                self.insertion_dict[self.curr_name] = 1
            else:
                self.insertion_dict[self.curr_name] = self.insertion_dict[self.curr_name] + 1
            insert_counter = self.insertion_dict[self.curr_name]
            self.curr_name = self.curr_name + "_INSERTION_{}".format(insert_counter)
            q = self._find_select(q)
        elif q.find("DELETE FROM") != -1:
            # find the current name in the insertion dict and how many times it has been deleted
            self.curr_name = re.sub(rem_regex, "", q.split(" ")[2])
            if self.curr_name not in self.deletion_dict.keys():
                self.deletion_dict[self.curr_name] = 1
            else:
                self.deletion_dict[self.curr_name] = self.deletion_dict[self.curr_name] + 1
            delete_counter = self.deletion_dict[self.curr_name]
            self.curr_name = self.curr_name + "_DELETION_{}".format(delete_counter)
            q = self._find_select(q)
        elif q.find("COPY") != -1:
            self.curr_name = q.split(" ")[1]
        elif q.find("ANALYZE") != -1:
            self.curr_name = q.split(" ")[1]
            self.curr_name = re.sub(rem_regex, "", self.curr_name)
            if self.curr_name not in self.analyze_list:
                self.analyze_list.append(self.curr_name)
            # Change the name of the table to ANALYZED
            for key, value in self.output_dict.copy().items():
                temp_value = []
                for w in value:
                    if w == self.curr_name:
                        temp_value.append(w + '_ANALYZED')
                    else:
                        temp_value.append(w)
                self.output_dict[key] = temp_value
            if self.curr_name + "_ANALYZED" not in self.output_dict.keys():
                self.output_dict[self.curr_name + "_ANALYZED"] = self.output_dict[self.curr_name]
                self.output_dict.pop(self.curr_name, None)
            self.curr_name = self.curr_name + "_ANALYZED"
        else:
            self.curr_name = str(idx)
        return q

    def _produce_html(self, output_json: str = ""):
        # Creating the HTML file
        file_html = open("index.html", "w", encoding="utf-8")
        # Adding the input data to the HTML file
        file_html.write('''<!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <meta http-equiv="X-UA-Compatible" content="ie=edge">
      <title>DTDesign-React血缘图组件</title>
    </head>
    <body>
      <script>
        window.inlineSource = `{}`;
      </script>
      <div id="main"></div>
    <script type="text/javascript" src="app.js"></script></body>
    </html>'''.format(output_json))
        # Saving the data into the HTML file
        file_html.close()

    def _find_select(self, q):
        if q.find("SELECT") != -1:
            idx = q.find("SELECT")
            q = q[idx:]
        else:
            q = q
        return q

if __name__ == "__main__":
    #sql = ["WITH agetbl AS ( SELECT ad.subject_id FROM mimiciii_clinical.admissions ad INNER JOIN patients p ON ad.subject_id = p.subject_id WHERE DATETIME_DIFF(ad.admittime, p.dob, 'YEAR'::TEXT) > 15 group by ad.subject_id HAVING ad.subject_id > 5 ),bun as ( SELECT width_bucket(valuenum,0,280,280) AS bucket,le.* FROM mimiciii_clinical.labevents le INNER JOIN agetbl ON le.subject_id = agetbl.subject_id WHERE itemid IN (51006) ) SELECT bucket as blood_urea_nitrogen,count(bun.*) as c FROM bun GROUP BY bucket ORDER BY bucket;", "DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';", "COPY Customers FROM STDIN DELIMITER '|' ENCODING 'UTF-8'", "ANALYZE Customers;"]
    sql = [
        "COPY Customers FROM STDIN DELIMITER '|' ENCODING 'UTF-8'",
        "DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';",
        "DELETE FROM Customers WHERE CustomerName='Kate';", "ANALYZE Customers;",
    "SELECT * FROM Customers", "ANALYZE Customers", "INSERT INTO Customers", "COPY Sales FROM STDIN DELIMITER '|' ENCODING 'UTF-8'"]
    #input_table_dict = {"mimiciii_clinical.labevents": ['itemid', 'valuenum', 'subject_id']}
    ColumnLineageNoConn(sql)
