# -*- coding: utf-8 -*-
"""
    cli.volume.py

    :copyright: (c) 2014 by Aravinda VK
    :license: MIT, see LICENSE for more details.
"""
import re
from glusterfsrest.cli import volume
from glusterfsrest import utils
from glusterfsrest.exceptions import GlusterCliBadXml, ParseError
from glusterfsrest.exceptions import GlusterCliFailure


SNAPSHOT_CMD = ['gluster', '--mode=script', 'snapshot']


def _parse_a_vol(snapshot_el):
    value = {
        'name': snapshot_el.find('name').text,
        'uuid': snapshot_el.find('uuid').text,
	'description': snapshot_el.find('description').text,
        'createTime': snapshot_el.find('createTime').text,
        'volCount': snapshot_el.find('volCount').text,
	'snapVolumeName': snapshot_el.find('snapVolume').find('name').text,
        'snapVolumeStatus': snapshot_el.find('snapVolume').find('status').text,
        'snapOriginName': snapshot_el.find('snapVolume').find('originVolume').find('name').text,
        'snapOriginSnapCount': snapshot_el.find('snapVolume').find('originVolume').find('snapCount').text,
        'snapOriginSnapRemaining': snapshot_el.find('snapVolume').find('originVolume').find('snapRemaining').text,
        'options': []
    }

    return value

def _parseCreateOutput(createResponse):
#    matchObj = re.match( r'snapshot create: ([a-zA-Z]+): Snap ([0-9a-zA-Z_\-\.]+)', createResponse, re.M|re.I)
    tree = utils.checkxmlcorrupt(createResponse)
    #el = tree.find('snapshot/name').text
    if tree.find('opErrno').text == "0":
        return tree.find('snapCreate/snapshot/name').text
    else:
        raise GlusterCliFailure("Snapshot not created %s" % tree.find('opErrstr').text)

    raise GlusterCliFailure("Snapshot creation failed!: %s" % createResponse)
    

def _parseinfo(snapinfo):
    tree = utils.checkxmlcorrupt(snapinfo)
    snapshots = []
    for el in tree.findall('snapInfo/snapshots/snapshot'):
        try:
            snapshots.append(_parse_a_vol(el))
        except (ParseError, AttributeError, ValueError) as e:
            raise GlusterCliBadXml(str(e))

    return snapshots


def info(name=None):
    cmd = SNAPSHOT_CMD + ["info"] + ([name] if name else [])
    data = utils.execute_and_output(cmd, _parseinfo)
    if name and not data:
        raise GlusterCliFailure("Volume %s does not exist" % name)

    return data

def activate(name, force=False):
    cmd = SNAPSHOT_CMD + ["activate", name]
    if force:
        cmd += ["force"]

    return utils.checkstatuszero(cmd)

def deactivate(name, force=False):
    cmd = SNAPSHOT_CMD + ["deactivate", name]
    if force:
        cmd += ["force"]

    return utils.checkstatuszero(cmd)

def restore(name):
    cmd = SNAPSHOT_CMD + ["restore", name]

    return utils.checkstatuszero(cmd)

def create(volName, snapName, description='', force=False,
           activate_snapshot=False):
    cmd = SNAPSHOT_CMD + ["create", snapName, volName]
    if description != '':
        cmd += ["description", str(description)]

    if force:
        cmd += ["force"]

    # If volume needs to be started, then run create command without
    # decorator else return create command and statuszerotrue
    # decorator will take care of running cmd
    data = utils.execute_and_output(cmd, _parseCreateOutput)

    if activate_snapshot:
        if not data:
          raise GlusterCliFailure("Snapshot not created %s does not exist" % snapName)
#        utils.checkstatuszero(cmd)
        else:
            activate(data)
    else:
        utils.checkstatuszero(cmd)

    return data


def delete(name, deactivate_snapshot=False):
    if deactivate_snapshot:
        deactivate(name, force=True)

    cmd = SNAPSHOT_CMD + ["delete", name]
    return utils.checkstatuszero(cmd)

def clone(cloneName, snapName, description='', force=False,start=False):
    cmd = SNAPSHOT_CMD + ["clone", cloneName, snapName]
    if description != '':
        cmd += ["description", str(description)]

    if force:
        cmd += ["force"]

    utils.checkstatuszero(cmd)

    if start:
        return volume.start(cloneName)

