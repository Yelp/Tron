"""Functions either used by more than one executable or used by tests as well
as executables
"""

def make_job_to_uri(content):
    """Use ``content`` (the result of the '/' API call) to generate a dict
    mapping job names to URIs
    """
    return dict([(job['name'], job['href']) for job in content['jobs']])

def make_service_to_uri(content):
    """Use ``content`` (the result of the '/' API call) to generate a dict
    mapping service names to URIs
    """
    return dict([(service['name'], service['href']) for service in content['services']])

def obj_spec_to_uri(obj_spec, job_to_uri, service_to_uri):
    """Convert a string of the form job_name[.run_number[.action]] to its
    corresponding URL
    """
    obj_name_elements = obj_spec.split('.')
    obj_name = obj_name_elements[0]
    obj_rel_path = "/".join(obj_name_elements[1:])

    obj_uri = None
    if obj_name in job_to_uri:
        obj_uri = job_to_uri[obj_name]
    elif obj_name in service_to_uri:
        obj_uri = service_to_uri[obj_name]

    if not obj_uri:
        raise Exception("Unknown identifier")

    return '/'.join((obj_uri, obj_rel_path))
