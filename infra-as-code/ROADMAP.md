

I am presenting a 30_000 feet view of the infra structure roadmap.
Please note this roadmap is strictly limited to the IaaS (Infra-as-a-Service)
level with some overlap with PaaS (Platform-as-a-Serice)

It does not attempt to cover any roadmap of the software / applications
developemnt that you would be doing as a part of the company technology
roadmap.


RoadMap Milestones


1>> Review of all services at baseline level.

We need to gauge the defination of  "working state"
All the application services that are required to be in running
state for a smooth execution of business constitute the
working state.

2>> Setting up of monitoring infra (eg Zabbix or Nagios)

Once (1) is established we need to place the monitoring of
services in place. The monitoring parameters shall be at least
linked to the business functions whose availability is directly
perceived while transacting the business.

Apart from such params ,health related params shall also
be monitored as a part of preventive strategy.

3>> Arrangement of DR

An arrangement of backup mechanism is an important pillar of any IT setup.
We need to setup backup policies and mechanisim and setup monitoring
for the same so that the failure of compliance is escalated.


4>> Configuration Automation

This involves setting up tools like Chef , Puppet , Salt , Ansible etc that
implements the concept of "Infra as a Code"

The end result is that that the effort of configuration of nodes in a cloud
environment is minimized. This also lays the foundations for implementation of
auto-scalaing

5>> Continuos Integration
The method or process of deployment of softwares changes to
production environment should be  automated as far as possible.



AUTHOR: Rajesh Kumar Mallah <mallah@redgrape.tech>
Redgrape Technologies ( https://www.redgrape.tech )
