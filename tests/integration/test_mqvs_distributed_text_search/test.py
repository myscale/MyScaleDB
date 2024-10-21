import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# shard1
node_s1_r1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
node_s1_r2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)

# shard2
node_s2_r1 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
node_s2_r2 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)

data = [
        (1, 'The mayor announced a new initiative to revitalize the downtown area. This project will include the construction of new parks and the renovation of historic buildings.'),
        (2, 'Local schools are introducing a new curriculum focused on science and technology. The goal is to better prepare students for careers in STEM fields.'),
        (3, 'A new community center is opening next month, offering a variety of programs for residents of all ages. Activities include fitness classes, art workshops, and social events.'),
        (4, 'The city council has approved a plan to improve public transportation. This includes expanding bus routes and adding more frequent services during peak hours.'),
        (5, 'A new library is being built in the west side of the city. The library will feature modern facilities, including a digital media lab and community meeting rooms.'),
        (6, 'The local hospital has received funding to upgrade its emergency department. The improvements will enhance patient care and reduce wait times.'),
        (7, 'A new startup accelerator has been launched to support tech entrepreneurs. The program offers mentoring, networking opportunities, and access to investors.'),
        (8, 'The city is hosting a series of public workshops on climate change. The sessions aim to educate residents on how to reduce their carbon footprint.'),
        (9, 'A popular local restaurant is expanding with a new location in the downtown area. The restaurant is known for its farm-to-table cuisine and sustainable practices.'),
        (10, 'The annual arts festival is set to begin next week, featuring performances, exhibitions, and workshops by local and international artists.'),
        (11, 'The city is implementing new measures to improve air quality, including stricter emissions standards for industrial facilities and incentives for electric vehicles.'),
        (12, 'A local nonprofit is organizing a food drive to support families in need. Donations of non-perishable food items can be dropped off at designated locations.'),
        (13, 'The community garden project is expanding, with new plots available for residents to grow their own vegetables and herbs. The garden promotes healthy eating and sustainability.'),
        (14, 'The police department is introducing body cameras for officers to increase transparency and accountability. The initiative is part of broader efforts to build trust with the community.'),
        (15, 'A new public swimming pool is opening this summer, offering swimming lessons and recreational activities for all ages. The pool is part of a larger effort to promote health and wellness.'),
        (16, 'The city is launching a campaign to promote recycling and reduce waste. Residents are encouraged to participate by recycling household items and composting organic waste.'),
        (17, 'A local theater group is performing a series of classic plays at the community center. The performances aim to make theater accessible to a wider audience.'),
        (18, 'The city is investing in renewable energy projects, including the installation of solar panels on public buildings and the development of wind farms.'),
        (19, 'A new sports complex is being built to provide facilities for basketball, soccer, and other sports. The complex will also include a fitness center and walking trails.'),
        (20, 'The city is hosting a series of workshops on financial literacy, aimed at helping residents manage their money and plan for the future. Topics include budgeting, saving, and investing.'),
        (21, 'A new art exhibit is opening at the city museum, featuring works by contemporary artists from around the world. The exhibit aims to foster a greater appreciation for modern art.'),
        (22, 'The local animal shelter is holding an adoption event this weekend. Dogs, cats, and other pets are available for adoption, and volunteers will be on hand to provide information.'),
        (23, 'The city is upgrading its water infrastructure to ensure a reliable supply of clean water. The project includes replacing old pipes and installing new treatment facilities.'),
        (24, 'A new technology incubator has opened to support startups in the tech sector. The incubator provides office space, resources, and mentorship to help entrepreneurs succeed.'),
        (25, 'The city is planning to build a new bike lane network to promote cycling as a healthy and environmentally friendly mode of transportation. The project includes dedicated bike lanes and bike-sharing stations.'),
        (26, 'The local farmers market is reopening for the season, offering fresh produce, artisanal goods, and handmade crafts from local vendors.'),
        (27, 'A new educational program is being launched to support early childhood development. The program provides resources and training for parents and caregivers.'),
        (28, 'The city is organizing a series of concerts in the park, featuring performances by local bands and musicians. The concerts are free and open to the public.'),
        (29, 'A new senior center is opening, offering programs and services for older adults. Activities include fitness classes, educational workshops, and social events.'),
        (30, 'The city is implementing a new traffic management system to reduce congestion and improve safety. The system includes synchronized traffic lights and real-time traffic monitoring.'),
        (31, 'A new community outreach program is being launched to support at-risk youth. The program provides mentoring, tutoring, and recreational activities.'),
        (32, 'The city is hosting a series of public forums to discuss plans for future development. Residents are encouraged to attend and provide feedback on proposed projects.'),
        (33, 'A new public art installation is being unveiled in the downtown area. The installation features sculptures and murals by local artists and aims to beautify the urban landscape.'),
        (34, 'The local university is launching a new research center focused on sustainable development. The center will conduct research and provide education on environmental issues.'),
        (35, 'The city is planning to expand its public Wi-Fi network to provide free internet access in parks, libraries, and other public spaces.'),
        (36, 'A new community health clinic is opening, offering medical, dental, and mental health services. The clinic aims to provide affordable healthcare to underserved populations.'),
        (37, 'The city is implementing a new emergency alert system to provide residents with real-time information during emergencies. The system includes mobile alerts and social media updates.'),
        (38, 'A local nonprofit is organizing a job fair to connect job seekers with employers. The fair will feature workshops on resume writing, interview skills, and job search strategies.'),
        (39, 'The city is hosting a series of environmental cleanup events, encouraging residents to participate in efforts to clean up parks, rivers, and other natural areas.'),
        (40, 'A new fitness trail is being built in the local park, featuring exercise stations and informational signs to promote physical activity and wellness.'),
        (41, 'The city is launching a new initiative to support small businesses, offering grants, training, and resources to help entrepreneurs grow their businesses.'),
        (42, 'A new art school is opening, offering classes and workshops for aspiring artists of all ages. The school aims to foster creativity and provide a supportive environment for artistic development.'),
        (43, 'The city is planning to improve its public transportation system by introducing electric buses and expanding routes to underserved areas.'),
        (44, 'A new music festival is being organized to celebrate local talent and bring the community together. The festival will feature performances by local bands, food stalls, and family-friendly activities.'),
        (45, 'The city is implementing new measures to protect green spaces, including the creation of new parks and the preservation of existing natural areas.'),
        (46, 'A new housing project is being developed to provide affordable homes for low-income families. The project includes energy-efficient buildings and community amenities.'),
        (47, 'The city is hosting a series of workshops on entrepreneurship, providing training and resources for aspiring business owners. Topics include business planning, marketing, and finance.'),
        (48, 'A new public garden is being created to provide a space for residents to relax and enjoy nature. The garden will feature walking paths, benches, and a variety of plants.'),
        (49, 'The city is launching a campaign to promote public health, encouraging residents to get vaccinated, exercise regularly, and eat a balanced diet.'),
        (50, 'A new community theater is opening, offering performances, workshops, and classes for residents of all ages. The theater aims to make the performing arts accessible to everyone.')
    ]

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [node_s1_r1, node_s1_r2]:
            node.query(
                """
                CREATE TABLE IF NOT EXISTS local_table(id UInt32, text String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_1/local_table', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )
        
        for node in [node_s2_r1, node_s2_r2]:
            node.query(
                """
                CREATE TABLE IF NOT EXISTS local_table(id UInt32, text String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_2/local_table', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )

        node_s1_r1.query("CREATE TABLE distributed_table(id UInt32, text String) ENGINE = Distributed(test_cluster, default, local_table, id);")
        node_s1_r1.query("INSERT INTO distributed_table (id, text) VALUES {}".format(", ".join(map(str, data))))
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster ADD INDEX fts_ind text TYPE fts;")
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster MATERIALIZE INDEX fts_ind;")

        # Create a MergeTree table with the same data as the distributed table to serve as the ground truth for queries in the distributed TextSearch.
        node_s1_r1.query("CREATE TABLE text_table (id UInt32, text String) ENGINE = MergeTree ORDER BY id;")
        node_s1_r1.query("INSERT INTO text_table (id, text) VALUES {}".format(", ".join(map(str, data))))
        node_s1_r1.query("ALTER TABLE text_table ADD INDEX fts_ind text TYPE fts;")
        node_s1_r1.query("ALTER TABLE text_table MATERIALIZE INDEX fts_ind;")

        time.sleep(2)

        yield cluster

    finally:
        cluster.shutdown()

def test_distributed_text_search(started_cluster):
    # Test table function ftsIndex()
    assert node_s1_r1.query("SELECT * FROM ftsIndex(default, text_table, text, 'new') ORDER BY field_tokens ASC") == "50\t[(1,1267)]\t[('new',1,30)]\n"
    assert node_s1_r1.query("SELECT * FROM cluster(test_cluster, ftsIndex(default, local_table, text, 'new')) ORDER BY field_tokens ASC") == "25\t[(1,630)]\t[('new',1,19)]\n25\t[(1,637)]\t[('new',1,11)]\n"

    # Test query setting dfs_query_then_fetch does not affect the non-Distributed TextSearch results
    assert node_s1_r1.query("SELECT id, TextSearch(text, 'city') AS bm25 FROM text_table ORDER BY bm25 DESC LIMIT 5 SETTINGS dfs_query_then_fetch = 1") == node_s1_r1.query("SELECT id, TextSearch(text, 'city') AS bm25 FROM text_table ORDER BY bm25 DESC LIMIT 5 SETTINGS dfs_query_then_fetch = 0")

    # Test Distributed TextSearch results is the same as the non-Distributed TextSearch results
    assert node_s1_r1.query("SELECT id, TextSearch(text, 'city') AS bm25 FROM text_table ORDER BY bm25 DESC, id ASC LIMIT 5") == node_s1_r1.query("SELECT id, TextSearch(text, 'city') AS bm25 FROM distributed_table ORDER BY bm25 DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1")

    # Test Distributed TextSearch results with dfs_query_then_fetch = 0
    assert node_s1_r1.query("SELECT id, TextSearch(text, 'city') AS bm25 FROM distributed_table ORDER BY bm25 DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 0") == "18\t1.1643934\n4\t1.1452436\n8\t1.1267136\n16\t1.1267136\n30\t1.1087735\n"
