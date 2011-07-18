def make_job_to_uri(content):
    return dict([(job['name'], job['href']) for job in content['jobs']])

def make_service_to_uri(content):
    dict([(service['name'], service['href']) for service in content['services']])

def obj_spec_to_uri(obj_spec, job_to_uri, service_to_uri):
    obj_name_elements = obj_spec.split('.')
    obj_name = obj_name_elements[0]
    obj_rel_path = "/".join(obj_name_elements[1:])

    obj_uri = None
    if obj_name in job_to_uri:
        obj_uri = job_to_uri[obj_name]
    elif obj_name in service_to_uri:
        obj_uri = service_to_uri[obj_name]

    if not obj_uri:
        raise Exception("Unknown identifier: %s" % args[1])

    return '/'.join((obj_uri, obj_rel_path))
