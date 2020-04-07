from __future__ import unicode_literals

import logging
import os
import sys

import webapp2
from google.appengine.ext import ndb

cwd = os.getcwd()
sys.path.insert(0, 'includes')

from p1_datastores import Datastores
from datastore_functions import DatastoreFunctions as DSF
from datavalidation import DataValidation
from datastore_functions import ReplicateToFirebaseFlag
from p1_services import TaskArguments
from p1_global_settings import PostDataRules
from datastore_functions import ReplicateToDatastoreFlag
from GCP_return_codes import FunctionReturnCodes as RC
from GCP_datastore_logging import LoggingFuctions as LF
from task_queue_functions import TaskQueueFunctions


class CommonPostHandler(DataValidation):
    def post(self):
        task_id = "maintenance-tasks:CommonPostHandler:post"
        debug_data = []
        call_result = self.processPushTask()
        debug_data.append(call_result)
        task_results = call_result['task_results']

        params = {}
        for key in self.request.arguments():
            params[key] = self.request.get(key, None)
        task_functions = TaskQueueFunctions()

        if call_result['success'] != RC.success:
            task_functions.logError(
                call_result['success'], task_id, params,
                self.request.get('X-AppEngine-TaskName', None),
                self.request.get('transaction_id', None), call_result['return_msg'], debug_data,
                self.request.get('transaction_user_uid', None)
            )
            task_functions.logTransactionFailed(self.request.get('transaction_id', None), call_result['success'])
            if call_result['success'] < RC.retry_threshold:
                self.response.set_status(500)
            else:
                # any other failure scenario will continue to fail no matter how many times its called.
                self.response.set_status(200)
            return

        # go to the next function
        task_functions = TaskQueueFunctions()
        call_result = task_functions.nextTask(task_id, task_results, params)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            task_functions.logError(
                call_result['success'], task_id, params,
                self.request.get('X-AppEngine-TaskName', None),
                self.request.get('transaction_id', None), call_result['return_msg'], debug_data,
                self.request.get('transaction_user_uid', None)
            )
        # </end> go to the next function
        self.response.set_status(200)


class ReplicateDatastore(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "maintenance-tasks:ReplicateDatastore:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        datastore_name = unicode(self.request.get(TaskArguments.s6t1_datastore_name, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [datastore_name, PostDataRules.required_name],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        query = ndb.Query(kind=datastore_name)
        for row in query.iter():
            try:
                row.replicateEntityToFirebase()
            except Exception as exc:
                logging.error("EXCEPTION: {}".format(exc))

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


app = webapp2.WSGIApplication([
    ('/p1s6t1-replicate-datastore', ReplicateDatastore),
], debug=True)
