from setuptools import setup, find_packages


service_modules = find_packages(include=['src.service', 'src.service.*'])
event_modules = find_packages(include=['src.route_events', 'src.route_events.*'])

setup(
    name="route_events", 
    packages = 
    list(map(lambda x: x.replace('src.service', 'route_events_service'), service_modules)) + 
    list(map(lambda x: x.replace('src.', ''), event_modules)),
    package_dir={
        'route_events': "src/route_events",
        "route_events_service": "src/service"
    },
    package_data={'': ['*']}
)
