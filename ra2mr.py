from enum import Enum
import json

import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse
import re

'''
Control where the input data comes from, and where output data should go.
'''


def cmp(string1, string2):
    return [c for c in string1 if c.isalnum()] == [c for c in string2 if c.isalnum()]


def extract_tabname_record(json_tuple):
    first_key = list(json_tuple.keys())[0]
    return first_key[:first_key.index(".")]


def extract_cond_joint(cond):
    """" returns a list L of condition """
    condition = re.sub("and", "", str(cond))
    cond_list = re.findall(r"[\w.\w]+", condition)
    n = len(cond_list)
    return [(cond_list[i], cond_list[i + 1]) for i in range(0, n - 1, 2)]


def sort_cond(term):
    """inverse the cond a=b to b=a if a is not AtteRef and b is AtteRef and """
    if isinstance(term.inputs[1], radb.ast.AttrRef) and not isinstance(term.inputs[0], radb.ast.AttrRef):
        return radb.ast.ValExprBinaryOp(term.inputs[1], radb.ast.sym.EQ, term.inputs[0])
    else:
        return term


def clean_cond(cond):
    """this function insure condition commutativity  a=b <=> b=a"""
    if not isinstance(cond.inputs[0], radb.ast.ValExprBinaryOp):
        return sort_cond(cond)
    else:
        return radb.ast.ValExprBinaryOp(clean_cond(cond.inputs[0]), radb.ast.sym.AND, sort_cond(cond.inputs[1]))


def clean_select(ra):
    """select consistent with condition commutation"""
    return radb.ast.Select(clean_cond(ra.cond), ra.inputs[0])


def get_table(ra):
    """returns a tuple (relname,rel)"""
    if isinstance(ra, radb.ast.Select):
        if isinstance(ra.inputs[0], radb.ast.RelRef):
            return ra.inputs[0].rel
        else:
            return ra.inputs[0].inputs[0].rel
    if isinstance(ra, radb.ast.Rename):
        return ra.relname, ra.inputs[0].rel


def extract_cond(table_name, cond):
    """returns a list of tuple(s) each tuple contains the 2 terms of a condition"""
    condition = re.sub("and", "", str(cond))
    cond_list = re.findall(r"[\w']+[.|\s][\w']+|[\w']+[\d']*|[\w']+[\w']", condition)
    L = []
    n = len(cond_list)
    for i in range(0, n - 1, 2):
        e1 = table_name + "." + cond_list[i] if cond_list[i].count(".") == 0 else cond_list[i]
        e2 = cond_list[i + 1]
        L.append((e1, e2))
    return L


class ExecEnv(Enum):
    LOCAL = 1  # read/write local files
    HDFS = 2  # read/write HDFS
    MOCK = 3  # read/write mock data to an in-memory file system.


'''
Switches between different execution environments and file systems.
'''


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''


def count_steps(raquery):
    assert (isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery, radb.ast.Project) or
            isinstance(raquery, radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)

    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''


def task_factory(raquery, step=1, env=ExecEnv.HDFS, optimize=True):
    assert (isinstance(raquery, radb.ast.Node))

    if isinstance(raquery, radb.ast.Select) and isinstance(raquery.inputs[0], radb.ast.Rename):
        return SelectRenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JointSelect(querystring=str(raquery) + ";", step=step, exec_environment=env)

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    # elif isinstance(raquery, radb.ast.Join):
    #     return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")


class JointSelect(RelAlgQueryTask):
    ###not working :( I should implement a simple joint class with two inputs of type (select, rename)
    ### otherwise let the normal joint does the work
    def requires(self):
        ra = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(ra, radb.ast.Join))

        task1 = task_factory(ra.inputs[0], step=self.step + 1, env=self.exec_environment)
        task2 = task_factory(ra.inputs[1], step=self.step + count_steps(ra.inputs[0]) + 1,
                             env=self.exec_environment)
        if isinstance(ra.inputs[0], radb.ast.Select):
            if isinstance(ra.inputs[0].inputs[0], radb.ast.Rename):
                task1 = task_factory(ra.inputs[0].inputs[0].inputs[0], step=self.step + 1, env=self.exec_environment)
        elif isinstance(ra.inputs[1], radb.ast.Select):
            if isinstance(ra.inputs[1].inputs[0], radb.ast.Rename):
                task2 = task_factory(ra.inputs[1].inputs[0].inputs[0], step=self.step + count_steps(ra.inputs[0]) + 1,
                                 env=self.exec_environment)
        if isinstance(ra.inputs[0], radb.ast.Rename):
            task1 = task_factory(ra.inputs[0].inputs[0], step=self.step + 1, env=self.exec_environment)
        elif isinstance(ra.inputs[1], radb.ast.Rename):
            task2 = task_factory(ra.inputs[1].inputs[0], step=self.step + count_steps(ra.inputs[0]) + 1,
                                 env=self.exec_environment)
        return [task1, task2]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)
        ra = radb.parse.one_statement_from_string(self.querystring)

        input0 = ra.inputs[0]
        input1 = ra.inputs[1]
        if isinstance(input0, radb.ast.Join):
            res = json.dumps(json_tuple)
            yield ("join", res)

        if isinstance(input0, radb.ast.Select):
            input0 = clean_select(input0)
            condition = input0.cond
            if isinstance(input0.inputs[0], radb.ast.Rename):
                rename, real_name = get_table(input0.inputs[0])
                if real_name == relation:
                    d = {}
                    key_list = json_tuple.keys()
                    new_key_list = [e.replace(relation + ".", rename + ".") for e in key_list]
                    values_list = json_tuple.values()
                    d = {x: y for x, y in zip(new_key_list, values_list)}

                    first_key = list(d.keys())[0]
                    table_name = first_key[:first_key.index(".")]
                    cond_list = extract_cond(table_name, condition)
                    test = True
                    for c1, c2 in cond_list:
                        if not cmp(str(d[c1]), c2):
                            test = False
                            break
                    if test:
                        res = json.dumps(d)
                        yield (relation, res)
        elif isinstance(input0, radb.ast.Rename):
            rename, real_name = get_table(input0)
            if real_name == relation:
                d = {}
                key_list = json_tuple.keys()
                new_key_list = [e.replace(relation + ".", rename + ".") for e in key_list]
                values_list = json_tuple.values()
                d = {x: y for x, y in zip(new_key_list, values_list)}
                res = json.dumps(d)
                yield (relation, res)
        # else:
        #     ra = json.dumps(json_tuple)
        #     yield ("join", ra)

        if isinstance(input1, radb.ast.Select):
            input1 = clean_select(input1)
            condition = input1.cond
            if isinstance(input1.inputs[0], radb.ast.Rename):
                rename, real_name = get_table(input1.inputs[0])
                if real_name == relation:
                    d = {}
                    key_list = json_tuple.keys()
                    new_key_list = [e.replace(relation + ".", rename + ".") for e in key_list]
                    values_list = json_tuple.values()
                    d = {x: y for x, y in zip(new_key_list, values_list)}

                    first_key = list(d.keys())[0]
                    table_name = first_key[:first_key.index(".")]
                    cond_list = extract_cond(table_name, condition)
                    test = True
                    for c1, c2 in cond_list:
                        if not cmp(str(d[c1]), c2):
                            test = False
                            break
                    if test:
                        res = json.dumps(d)
                        yield (relation, res)
        elif isinstance(input1, radb.ast.Rename):
            rename, real_name = get_table(input1)
            if real_name == relation:
                d = {}
                key_list = json_tuple.keys()
                new_key_list = [e.replace(relation + ".", rename + ".") for e in key_list]
                values_list = json_tuple.values()
                d = {x: y for x, y in zip(new_key_list, values_list)}
                res = json.dumps(d)
                yield (relation, res)
        # else:
        #     ra = json.dumps(json_tuple)
        #     yield ("join", ra)

    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond
        L = [json.loads(e) for e in values]
        list_cond = []
        list_cond = extract_cond_joint(condition)
        first_key = list_cond[0][0]
        table_name1 = first_key[:first_key.index(".")]
        second_key = list_cond[0][1]
        table_name2 = second_key[:second_key.index(".")]
        joint_list1, joint_list2 = [], []
        for e in L:
            if extract_tabname_record(e) == table_name2:
                joint_list2.append(e)
            else:
                joint_list1.append(e)
        for e1 in joint_list1:
            for e2 in joint_list2:
                test = True
                for c1, c2 in list_cond:
                    if e1[c1] != e2[c2]:
                        test = False
                        break
                if test:
                    d = {x: y for x, y in zip(list(e1.keys()) + list(e2.keys()), list(e1.values()) + list(e2.values()))}
                    res = json.dumps(d)
                    yield ('joint1', res)


class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Join))

        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment)

        return [task1, task2]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond
        res = json.dumps(json_tuple)
        yield ("join", res)

    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond
        L = [json.loads(e) for e in values]
        list_cond = []
        list_cond = extract_cond_joint(condition)
        first_key = list_cond[0][0]
        table_name1 = first_key[:first_key.index(".")]
        second_key = list_cond[0][1]
        table_name2 = second_key[:second_key.index(".")]
        joint_list1, joint_list2 = [], []
        for e in L:
            if extract_tabname_record(e) == table_name2:
                joint_list2.append(e)
            else:
                joint_list1.append(e)
        for e1 in joint_list1:
            for e2 in joint_list2:
                test = True
                for c1, c2 in list_cond:
                    if e1[c1] != e2[c2]:
                        test = False
                        break
                if test:
                    d = {x: y for x, y in zip(list(e1.keys()) + list(e2.keys()), list(e1.values()) + list(e2.values()))}
                    res = json.dumps(d)
                    yield ('joint1', res)


class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Select))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)
        ra = radb.parse.one_statement_from_string(self.querystring)
        ra = clean_select(ra)
        condition = ra.cond

        first_key = list(json_tuple.keys())[0]
        table_name = first_key[:first_key.index(".")]
        cond_list = extract_cond(table_name, condition)
        test = True
        for c1, c2 in cond_list:
            if not cmp(str(json_tuple[c1]), c2):
                test = False
                break
        if test:
            yield (relation, tuple)


class SelectRenameTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Select))

        return [task_factory(raquery.inputs[0].inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)
        ra = radb.parse.one_statement_from_string(self.querystring)
        ra = clean_select(ra)
        condition = ra.cond

        rename, real_name = get_table(ra.inputs[0])
        if real_name == relation:
            d = {}
            key_list = json_tuple.keys()
            new_key_list = [e.replace(relation + ".", rename + ".") for e in key_list]
            values_list = json_tuple.values()
            d = {x: y for x, y in zip(new_key_list, values_list)}

        first_key = list(d.keys())[0]
        table_name = first_key[:first_key.index(".")]
        cond_list = extract_cond(table_name, condition)
        test = True
        for c1, c2 in cond_list:
            if not cmp(str(d[c1]), c2):
                test = False
                break
        if test:
            res = json.dumps(d)
            yield (relation, res)


class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        ra = radb.parse.one_statement_from_string(self.querystring)
        rename, real_name = get_table(ra)
        if real_name == relation:
            d = {}
            key_list = json_tuple.keys()
            new_key_list = [e.replace(relation + ".", rename + ".") for e in key_list]
            values_list = json_tuple.values()
            d = {x: y for x, y in zip(new_key_list, values_list)}
            res = json.dumps(d)
            yield (relation, res)


class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        attrs = radb.parse.one_statement_from_string(self.querystring).attrs
        first_key = list(json_tuple.keys())[0]
        table_name = first_key[:first_key.index(".")]

        attributes = [str(att) if att.rel is not None else table_name + "." + att.name for att in attrs]
        d = {}
        for k, v in json_tuple.items():
            if k in attributes:
                d[k] = v
        if len(d) != 0:
            res = json.dumps(d)
            yield (relation, res)

    def reducer(self, key, values):
        new_values = set(values)
        for e in new_values:
            yield (key, e)


if __name__ == '__main__':
    luigi.run()
