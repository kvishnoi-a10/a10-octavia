#    Copyright 2019, A10 Networks
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
from acos_client import errors as acos_errors
from oslo_config import cfg
from oslo_log import log as logging
from requests import exceptions
from taskflow import task
from octavia.common import constants
from octavia.controller.worker.v2.tasks import lifecycle_tasks

from a10_octavia.common import openstack_mappings
from a10_octavia.controller.worker.tasks.decorators import axapi_client_decorator
from a10_octavia.controller.worker.tasks.policy import PolicyUtil
from a10_octavia.controller.worker.tasks import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class L7RuleParent(object):

    def set(self, l7rule, l7policy, listeners):
        filename = l7policy[constants.L7POLICY_ID]
        p = PolicyUtil()
        script = p.createPolicy(l7policy)
        size = len(script.encode('utf-8'))
        listener = listeners[0]
        c_pers, s_pers = utils.get_sess_pers_templates(listener.get('default_pool'))
        tcp_proxy, aflex = utils.get_tcp_proxy_template(listener, listener.get('default_pool'))
        kargs = {}
        listener[constants.PROTOCOL] = openstack_mappings.virtual_port_protocol(self.axapi_client,
                                                                     listener[constants.PROTOCOL])
        try:
            self.axapi_client.slb.aflex_policy.create(
                file=filename, script=script, size=size, action="import")
            LOG.debug("Successfully created l7 rule: %s", l7rule[constants.L7RULE_ID])
        except (acos_errors.ACOSException, exceptions.ConnectionError) as e:
            LOG.exception("Failed to create/update l7rule: %s", l7rule[constants.L7RULE_ID])
            raise e

        try:
            get_listener = self.axapi_client.slb.virtual_server.vport.get(
                listener[constants.LOADBALANCER_ID], listener[constants.LISTENER_ID],
                listener[constants.PROTOCOL], listener['protocol_port'])
            LOG.debug("Successfully fetched listener %s for l7rule %s", listener[constants.LISTENER_ID], l7rule[constants.L7RULE_ID])
        except (acos_errors.ACOSException, exceptions.ConnectionError) as e:
            LOG.exception("Failed to get listener %s for l7rule: %s", listener[constants.LISTENER_ID], l7rule[constants.L7RULE_ID])
            raise e

        if 'aflex-scripts' in get_listener['port']:
            aflex_scripts = get_listener['port']['aflex-scripts']
            aflex_scripts.append({"aflex": filename})
        else:
            aflex_scripts = [{"aflex": filename}]
        kargs["aflex_scripts"] = aflex_scripts

        try:
            self.axapi_client.slb.virtual_server.vport.update(
                listener[constants.LOADBALANCER_ID], listener[constants.LISTENER_ID],
                listener[constants.PROTOCOL], listener['protocol_port'],
                listener.get('default_pool_id'), s_pers,
                c_pers, 1, tcp_proxy_name=tcp_proxy, **kargs)
            LOG.debug("Successfully associated l7rule %s to listener %s", l7rule[constants.L7RULE_ID], listener[constants.LISTENER_ID])
        except (acos_errors.ACOSException, exceptions.ConnectionError) as e:
            LOG.exception("Failed to associate l7rule %s to listener %s", l7rule[constants.L7RULE_ID], listener[constants.LISTENER_ID])
            raise e


class CreateL7Rule(L7RuleParent, task.Task):
    """Task to create L7Rule"""

    @axapi_client_decorator
    def execute(self, l7rule, l7policy, listeners, vthunder):
        self.set(l7rule,l7policy, listeners)


class UpdateL7Rule(L7RuleParent, task.Task):
    """Task to update L7Rule"""

    @axapi_client_decorator
    def execute(self, l7rule, l7policy, listeners, vthunder, update_dict):
        l7rule.update(update_dict)
        self.set(l7rule,l7policy,listeners)


class DeleteL7Rule(task.Task):
    """Task to delete a L7rule and disassociate from provided pool"""

    @axapi_client_decorator
    def execute(self, l7rule,l7policy, listeners, vthunder):
        if self.axapi_client and self.axapi_client.slb:
            # Use l7policy from method argument
            rules = l7policy.get(constants.L7RULES, [])

            # Safely remove the rule if it exists
            for index, rule in enumerate(rules):
                if hasattr(rule, "id") and rule.id == l7rule[constants.L7RULE_ID]:
                    del rules[index]
                    break

            l7policy[constants.L7RULES] = rules
            filename = l7policy[constants.L7POLICY_ID]
            p = PolicyUtil()
            script = p.createPolicy(l7policy)
            size = len(script.encode('utf-8'))
            listener = listeners[0]
            c_pers, s_pers = utils.get_sess_pers_templates(listener.get('default_pool'))
            tcp_proxy, aflex = utils.get_tcp_proxy_template(listener, listener.get('default_pool'))
            kargs = {}
            listener[constants.PROTOCOL] = openstack_mappings.virtual_port_protocol(self.axapi_client,
                                                                        listener[constants.PROTOCOL])
            try:
                self.axapi_client.slb.aflex_policy.create(
                    file=filename, script=script, size=size, action="import")
                LOG.debug("Successfully deleted l7rule: %s", l7rule[constants.L7RULE_ID])
            except (acos_errors.ACOSException, exceptions.ConnectionError) as e:
                LOG.warning("Failed to delete l7rule: %s", str(e))
                raise e

            try:
                get_listener = self.axapi_client.slb.virtual_server.vport.get(
                    (listener.get(constants.LOADBALANCER_ID) or listener.get(constants.LOAD_BALANCER_ID)), listener[constants.LISTENER_ID],
                    listener[constants.PROTOCOL], listener['protocol_port'])
                LOG.debug("Successfully fetched listener %s for l7rule %s", listener[constants.LISTENER_ID], l7rule[constants.L7RULE_ID])
            except (acos_errors.ACOSException, exceptions.ConnectionError) as e:
                LOG.exception("Failed to get listener %s for l7rule: %s", listener[constants.LISTENER_ID], l7rule[constants.L7RULE_ID])
                raise e

            if 'aflex-scripts' in get_listener['port']:
                aflex_scripts = get_listener['port']['aflex-scripts']
                aflex_scripts.append({"aflex": filename})
            else:
                aflex_scripts = [{"aflex": filename}]
            kargs["aflex_scripts"] = aflex_scripts

            try:
                self.axapi_client.slb.virtual_server.vport.update(
                    listener[constants.LOADBALANCER_ID], listener[constants.LISTENER_ID],
                    listener[constants.PROTOCOL], listener['protocol_port'], listener.get('default_pool_id'),
                    s_pers, c_pers, 1, tcp_proxy_name=tcp_proxy, **kargs)
                LOG.debug("Successfully dissociated l7rule %s from listener %s", l7rule[constants.L7RULE_ID], listener[constants.LISTENER_ID])
            except (acos_errors.ACOSException, exceptions.ConnectionError) as e:
                LOG.exception(
                    "Failed to dissociate l7rule %s from listener %s",
                    l7rule[constants.L7RULE_ID],
                    listener[constants.LISTENER_ID])
                raise e


class L7RuleToErrorOnRevertTask(lifecycle_tasks.BaseLifecycleTask):
    """Task to set a l7rule to ERROR on revert."""

    def execute(self, l7rule):
        pass

    def revert(self, l7rule, *args, **kwargs):
        try:
            self.task_utils.mark_l7rule_prov_status_error(l7rule[constants.L7RULE_ID])
        except Exception as e:
            LOG.exception("Failed to change status due to: %s", e)
