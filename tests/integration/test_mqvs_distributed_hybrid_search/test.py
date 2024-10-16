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
        (1, [1.0, 1.0, 1.0, 1.0, 1.0], 'The mayor announced a new initiative to revitalize the downtown area. This project will include the construction of new parks and the renovation of historic buildings.'),
        (2, [2.0, 2.0, 2.0, 2.0, 2.0], 'Local schools are introducing a new curriculum focused on science and technology. The goal is to better prepare students for careers in STEM fields.'),
        (3, [3.0, 3.0, 3.0, 3.0, 3.0], 'A new community center is opening next month, offering a variety of programs for residents of all ages. Activities include fitness classes, art workshops, and social events.'),
        (4, [4.0, 4.0, 4.0, 4.0, 4.0], 'The city council has approved a plan to improve public transportation. This includes expanding bus routes and adding more frequent services during peak hours.'),
        (5, [5.0, 5.0, 5.0, 5.0, 5.0], 'A new library is being built in the west side of the city. The library will feature modern facilities, including a digital media lab and community meeting rooms.'),
        (6, [6.0, 6.0, 6.0, 6.0, 6.0], 'The local hospital has received funding to upgrade its emergency department. The improvements will enhance patient care and reduce wait times.'),
        (7, [7.0, 7.0, 7.0, 7.0, 7.0], 'A new startup accelerator has been launched to support tech entrepreneurs. The program offers mentoring, networking opportunities, and access to investors.'),
        (8, [8.0, 8.0, 8.0, 8.0, 8.0], 'The city is hosting a series of public workshops on climate change. The sessions aim to educate residents on how to reduce their carbon footprint.'),
        (9, [9.0, 9.0, 9.0, 9.0, 9.0], 'A popular local restaurant is expanding with a new location in the downtown area. The restaurant is known for its farm-to-table cuisine and sustainable practices.'),
        (10, [10.0, 10.0, 10.0, 10.0, 10.0], 'The annual arts festival is set to begin next week, featuring performances, exhibitions, and workshops by local and international artists.'),
        (11, [11.0, 11.0, 11.0, 11.0, 11.0], 'The city is implementing new measures to improve air quality, including stricter emissions standards for industrial facilities and incentives for electric vehicles.'),
        (12, [12.0, 12.0, 12.0, 12.0, 12.0], 'A local nonprofit is organizing a food drive to support families in need. Donations of non-perishable food items can be dropped off at designated locations.'),
        (13, [13.0, 13.0, 13.0, 13.0, 13.0], 'The community garden project is expanding, with new plots available for residents to grow their own vegetables and herbs. The garden promotes healthy eating and sustainability.'),
        (14, [14.0, 14.0, 14.0, 14.0, 14.0], 'The police department is introducing body cameras for officers to increase transparency and accountability. The initiative is part of broader efforts to build trust with the community.'),
        (15, [15.0, 15.0, 15.0, 15.0, 15.0], 'A new public swimming pool is opening this summer, offering swimming lessons and recreational activities for all ages. The pool is part of a larger effort to promote health and wellness.'),
        (16, [16.0, 16.0, 16.0, 16.0, 16.0], 'The city is launching a campaign to promote recycling and reduce waste. Residents are encouraged to participate by recycling household items and composting organic waste.'),
        (17, [17.0, 17.0, 17.0, 17.0, 17.0], 'A local theater group is performing a series of classic plays at the community center. The performances aim to make theater accessible to a wider audience.'),
        (18, [18.0, 18.0, 18.0, 18.0, 18.0], 'The city is investing in renewable energy projects, including the installation of solar panels on public buildings and the development of wind farms.'),
        (19, [19.0, 19.0, 19.0, 19.0, 19.0], 'A new sports complex is being built to provide facilities for basketball, soccer, and other sports. The complex will also include a fitness center and walking trails.'),
        (20, [20.0, 20.0, 20.0, 20.0, 20.0], 'The city is hosting a series of workshops on financial literacy, aimed at helping residents manage their money and plan for the future. Topics include budgeting, saving, and investing.'),
        (21, [21.0, 21.0, 21.0, 21.0, 21.0], 'A new art exhibit is opening at the city museum, featuring works by contemporary artists from around the world. The exhibit aims to foster a greater appreciation for modern art.'),
        (22, [22.0, 22.0, 22.0, 22.0, 22.0], 'The local animal shelter is holding an adoption event this weekend. Dogs, cats, and other pets are available for adoption, and volunteers will be on hand to provide information.'),
        (23, [23.0, 23.0, 23.0, 23.0, 23.0], 'The city is upgrading its water infrastructure to ensure a reliable supply of clean water. The project includes replacing old pipes and installing new treatment facilities.'),
        (24, [24.0, 24.0, 24.0, 24.0, 24.0], 'A new technology incubator has opened to support startups in the tech sector. The incubator provides office space, resources, and mentorship to help entrepreneurs succeed.'),
        (25, [25.0, 25.0, 25.0, 25.0, 25.0], 'The city is planning to build a new bike lane network to promote cycling as a healthy and environmentally friendly mode of transportation. The project includes dedicated bike lanes and bike-sharing stations.'),
        (26, [26.0, 26.0, 26.0, 26.0, 26.0], 'The local farmers market is reopening for the season, offering fresh produce, artisanal goods, and handmade crafts from local vendors.'),
        (27, [27.0, 27.0, 27.0, 27.0, 27.0], 'A new educational program is being launched to support early childhood development. The program provides resources and training for parents and caregivers.'),
        (28, [28.0, 28.0, 28.0, 28.0, 28.0], 'The city is organizing a series of concerts in the park, featuring performances by local bands and musicians. The concerts are free and open to the public.'),
        (29, [29.0, 29.0, 29.0, 29.0, 29.0], 'A new senior center is opening, offering programs and services for older adults. Activities include fitness classes, educational workshops, and social events.'),
        (30, [30.0, 30.0, 30.0, 30.0, 30.0], 'The city is implementing a new traffic management system to reduce congestion and improve safety. The system includes synchronized traffic lights and real-time traffic monitoring.'),
        (31, [31.0, 31.0, 31.0, 31.0, 31.0], 'A new community outreach program is being launched to support at-risk youth. The program provides mentoring, tutoring, and recreational activities.'),
        (32, [32.0, 32.0, 32.0, 32.0, 32.0], 'The city is hosting a series of public forums to discuss plans for future development. Residents are encouraged to attend and provide feedback on proposed projects.'),
        (33, [33.0, 33.0, 33.0, 33.0, 33.0], 'A new public art installation is being unveiled in the downtown area. The installation features sculptures and murals by local artists and aims to beautify the urban landscape.'),
        (34, [34.0, 34.0, 34.0, 34.0, 34.0], 'The local university is launching a new research center focused on sustainable development. The center will conduct research and provide education on environmental issues.'),
        (35, [35.0, 35.0, 35.0, 35.0, 35.0], 'The city is planning to expand its public Wi-Fi network to provide free internet access in parks, libraries, and other public spaces.'),
        (36, [36.0, 36.0, 36.0, 36.0, 36.0], 'A new community health clinic is opening, offering medical, dental, and mental health services. The clinic aims to provide affordable healthcare to underserved populations.'),
        (37, [37.0, 37.0, 37.0, 37.0, 37.0], 'The city is implementing a new emergency alert system to provide residents with real-time information during emergencies. The system includes mobile alerts and social media updates.'),
        (38, [38.0, 38.0, 38.0, 38.0, 38.0], 'A local nonprofit is organizing a job fair to connect job seekers with employers. The fair will feature workshops on resume writing, interview skills, and job search strategies.'),
        (39, [39.0, 39.0, 39.0, 39.0, 39.0], 'The city is hosting a series of environmental cleanup events, encouraging residents to participate in efforts to clean up parks, rivers, and other natural areas.'),
        (40, [40.0, 40.0, 40.0, 40.0, 40.0], 'A new fitness trail is being built in the local park, featuring exercise stations and informational signs to promote physical activity and wellness.'),
        (41, [41.0, 41.0, 41.0, 41.0, 41.0], 'The city is launching a new initiative to support small businesses, offering grants, training, and resources to help entrepreneurs grow their businesses.'),
        (42, [42.0, 42.0, 42.0, 42.0, 42.0], 'A new art school is opening, offering classes and workshops for aspiring artists of all ages. The school aims to foster creativity and provide a supportive environment for artistic development.'),
        (43, [43.0, 43.0, 43.0, 43.0, 43.0], 'The city is planning to improve its public transportation system by introducing electric buses and expanding routes to underserved areas.'),
        (44, [44.0, 44.0, 44.0, 44.0, 44.0], 'A new music festival is being organized to celebrate local talent and bring the community together. The festival will feature performances by local bands, food stalls, and family-friendly activities.'),
        (45, [45.0, 45.0, 45.0, 45.0, 45.0], 'The city is implementing new measures to protect green spaces, including the creation of new parks and the preservation of existing natural areas.'),
        (46, [46.0, 46.0, 46.0, 46.0, 46.0], 'A new housing project is being developed to provide affordable homes for low-income families. The project includes energy-efficient buildings and community amenities.'),
        (47, [47.0, 47.0, 47.0, 47.0, 47.0], 'The city is hosting a series of workshops on entrepreneurship, providing training and resources for aspiring business owners. Topics include business planning, marketing, and finance.'),
        (48, [48.0, 48.0, 48.0, 48.0, 48.0], 'A new public garden is being created to provide a space for residents to relax and enjoy nature. The garden will feature walking paths, benches, and a variety of plants.'),
        (49, [49.0, 49.0, 49.0, 49.0, 49.0], 'The city is launching a campaign to promote public health, encouraging residents to get vaccinated, exercise regularly, and eat a balanced diet.'),
        (50, [50.0, 50.0, 50.0, 50.0, 50.0], 'A new community theater is opening, offering performances, workshops, and classes for residents of all ages. The theater aims to make the performing arts accessible to everyone.')
]

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [node_s1_r1, node_s1_r2]:
            node.query(
                """
                CREATE TABLE IF NOT EXISTS local_table(id UInt32, vector Array(Float32), text String, CONSTRAINT check_length CHECK length(vector) = 5) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_1/local_table', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )
        
        for node in [node_s2_r1, node_s2_r2]:
            node.query(
                """
                CREATE TABLE IF NOT EXISTS local_table(id UInt32, vector Array(Float32), text String, CONSTRAINT check_length CHECK length(vector) = 5) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_2/local_table', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )

        node_s1_r1.query("CREATE TABLE distributed_table(id UInt32, vector Array(Float32), text String, CONSTRAINT check_length CHECK length(vector) = 5) ENGINE = Distributed(test_cluster, default, local_table, id);")
        node_s1_r1.query("INSERT INTO distributed_table (id, vector, text) VALUES {}".format(", ".join(map(str, data))))
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster ADD INDEX fts_ind text TYPE fts;")
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster MATERIALIZE INDEX fts_ind;")

        # Create a MergeTree table with the same data as baseline.
        node_s1_r1.query("CREATE TABLE test_table (id UInt32, vector Array(Float32), text String, CONSTRAINT check_length CHECK length(vector) = 5) ENGINE = MergeTree ORDER BY id;")
        node_s1_r1.query("INSERT INTO test_table (id, vector, text) VALUES {}".format(", ".join(map(str, data))))
        node_s1_r1.query("ALTER TABLE test_table ADD INDEX fts_ind text TYPE fts;")
        node_s1_r1.query("ALTER TABLE test_table MATERIALIZE INDEX fts_ind;")

        time.sleep(2)

        yield cluster

    finally:
        cluster.shutdown()

def test_distributed_hybrid_search(started_cluster):

    ## Test RSF fusion_type
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")

    ## Test RRF fusion_type
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RRF')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'built') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RRF')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'built') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")

    ## Test enable_nlq and operator
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=false', 'operator=OR')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=false', 'operator=OR')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=false', 'operator=AND')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=false', 'operator=AND')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=true', 'operator=OR')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=true', 'operator=OR')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=true', 'operator=AND')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=true', 'operator=AND')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")
