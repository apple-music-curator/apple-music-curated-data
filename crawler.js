const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const glob = require('glob');
const { spawn } = require('child_process');

/**
 * ============================================================
 * Apple Music US Crawler + Artist Pipeline
 * ============================================================
 *
 * Pipeline:
 *  1) Depth-1 crawl
 *  2) Split discovered depth-2 URLs into 3 workers (parallel)
 *  3) Each depth-2 worker splits discovered depth-3 URLs into 3 workers (9 total, parallel)
 *  4) Merge crawl outputs
 *  5) Process BIG artist list in parallel workers using same browser+parallel model
 *  6) Final merge (crawl + artists)
 *
 * Concurrency model per worker process:
 *  - 10 browsers
 *  - 5 parallel tasks per browser
 *  - 50 task loops total
 */

const SEED_URLS = [
  'https://music.apple.com/us/top-charts',
  'https://music.apple.com/us/new/top-charts',
  'https://music.apple.com/us/radio',
  'https://music.apple.com/us/room/6760169562',
];

const MANDATORY_ARTISTS = [
  { name: 'Coco Jones', url: 'https://music.apple.com/us/artist/coco-jones/401400095' },
  { name: 'Amelia Moore', url: 'https://music.apple.com/us/artist/amelia-moore/1348611202' },
  { name: 'SAILORR', url: 'https://music.apple.com/us/artist/sailorr/1741604584' },
];

// User-provided huge artist list for post-crawl stage
const TOP_ARTISTS = [
  'Bruno Mars', 'Bad Bunny', 'The Weeknd', 'Rihanna', 'Taylor Swift',
  'Justin Bieber', 'Lady Gaga', 'Coldplay', 'Billie Eilish', 'Drake',
  'J Balvin', 'Ariana Grande', 'Ed Sheeran', 'David Guetta', 'Shakira',
  'Kendrick Lamar', 'Maroon 5', 'Eminem', 'Calvin Harris', 'SZA',
  'Kanye West', 'Pitbull', 'Daddy Yankee', 'Lana Del Rey', 'Dua Lipa',
  'Sabrina Carpenter', 'Katy Perry', 'Zara Larsson', 'Sia', 'Post Malone',
  'Michael Jackson', 'Olivia Dean', 'Rauw Alejandro', 'Harry Styles', 'sombr',
  'Travis Scott', 'Chris Brown', 'Sean Paul', 'Adele', 'Doja Cat',
  'Black Eyed Peas', 'Beyonce', 'RAYE', 'Future', 'Arctic Monkeys',
  'Arijit Singh', 'Imagine Dragons', 'Linkin Park', 'Ozuna', 'KAROL G',
  'Miley Cyrus', 'Alex Warren', 'Shreya Ghoshal', 'Djo', 'Sam Smith',
  'Halsey', 'Queen', 'Khalid', 'Fleetwood Mac', 'Marshmello',
  'Tate McRae', 'DJ Snake', 'The Chainsmokers', 'Ellie Goulding', 'Justin Timberlake',
  'Pritam', 'Don Omar', 'Lil Wayne', 'A.R. Rahman', 'Olivia Rodrigo',
  'Maluma', 'Don Toliver', 'Farruko', 'Elton John', 'Charlie Puth',
  'Britney Spears', 'The Neighbourhood', 'USHER', 'OneRepublic', 'Red Hot Chili Peppers',
  'Peso Pluma', '50 Cent', 'Ne-Yo', 'Tame Impala', 'Fuerza Regida',
  'Anuel AA', 'JENNIE', 'Shawn Mendes', 'Radiohead', 'Wiz Khalifa',
  'Nicki Minaj', 'One Direction', 'Playboi Carti', 'A$AP Rocky', 'Benson Boone',
  'Camila Cabello', 'Daniel Caesar', 'Metro Boomin', 'Teddy Swims', 'KPop Demon Hunters Cast',
  'Madonna', 'Hozier', 'The Police', 'J. Cole', 'Gorillaz',
  'Kesha', 'Flo Rida', 'Selena Gomez', 'JAY-Z', 'Romeo Santos',
  'Twenty One Pilots', '21 Savage', 'Disney', 'Tems', 'EJAE',
  'Akon', 'Myke Towers', 'Grupo Frontera', 'Tyler The Creator', 'REI AMI',
  'Kali Uchis', 'JHAYCO', 'Green Day', 'Frank Ocean', 'Feid',
  'HUNTR/X', 'ABBA', 'Nicky Jam', 'Anitta', 'AUDREY NUNA',
  'Swae Lee', 'Beele', 'The Marias', 'Avicii', 'Cardi B',
  'Nirvana', 'Alicia Keys', 'Gracie Abrams', 'Ty Dolla Sign', 'AC/DC',
  'Empire Of The Sun', 'Irshad Kamil', 'The Goo Goo Dolls', 'Tyla', 'Manuel Turizo',
  'P!nk', 'The Kid LAROI', 'PinkPantheress', 'Lil Uzi Vert', 'Sachin-Jigar',
  'Chappell Roan', 'Guns N Roses', 'Ovy On The Drums', 'Bebe Rexha', 'Tiesto',
  'Pharrell Williams', 'Charli xcx', 'Udit Narayan', 'The Beatles', 'XXXTENTACION',
  'Diplo', 'Enrique Iglesias', 'Tainy', 'Omar Courtz', 'Gunna',
  'Nelly Furtado', 'Demi Lovato', 'Dominic Fike', 'Chencho Corleone', 'ROSALIA',
  'Snoop Dogg', 'Paramore', 'Vishal-Shekhar', 'Mariah Carey', 'Morgan Wallen',
  'KATSEYE', 'Amitabh Bhattacharya', 'Arcancel', 'Metallica', 'Major Lazer',
  'Clean Bandit', 'Mac Miller', 'Bon Jovi', 'Juice WRLD', 'The Killers',
  'Creedence Clearwater Revival', 'Jennifer Lopez', 'Billy Joel', 'Cigarettes After Sex', 'Tinashe',
  'Tanishk Bagchi', 'Anne-Marie', 'Junior H', 'Lil Baby', 'Bizarrap',
  'Christina Aguilera', 'Shankar Mahadevan', 'Mark Ronson', 'Atif Aslam', 'James Arthur',
  'ZAYN', 'Danny Ocean', 'Ryan Castro', 'Mithoon', 'Cris MJ',
  'Kodak Black', 'Dei V', 'Childish Gambino', 'Neton Vega', 'Dave',
  'Jason Derulo', 'ROSE', 'Joji', 'Nengo Flow', 'Young Thug',
  'Noah Kahan', 'Tom Odell', 'Quevedo', 'Phil Collins', 'PARTYNEXTDOOR',
  'Miguel', 'Gigi Perez', 'Sonu Nigam', 'Macklemore', 'Whitney Houston',
  'Kehlani', 'Carin Leon', 'Ava Max', 'Wisin Yandel', 'Plan B',
  'Becky G', 'Luis Fonsi', 'Alan Walker', 'Zion Y Lennox', 'KK',
  'The Cranberries', 'Ravyn Lenae', 'The Script', 'GIVeON', 'Florence + The Machine',
  '2Pac', 'Oasis', 'El Alfa', 'Vishal Mishra', 'Alka Yagnik',
  'DaBaby', 'Chinmayi', 'Kid Cudi', 'Doechii', 'SIENNA SPIRO',
  'Bob Marley', 'Pink Floyd', 'TOTO', 'Bee Gees', 'Daft Punk',
  'Lewis Capaldi', 'French Montana', 'The Rolling Stones', 'Blessd', 'Mohit Chauhan',
  'Bryan Adams', 'Timbaland', 'Kygo', 'Sunidhi Chauhan', 'Tito Double P',
  'Lorde', 'Aventura', 'Shankar-Ehsaan-Loy', 'Backstreet Boys', 'Robin Schulz',
  'Sean Kingston', 'Aerosmith', 'Chris Stapleton', 'Burna Boy', 'Swedish House Mafia',
  'Luke Combs', 'Nickelback', 'Zach Bryan', 'Mana', 'Prince Royce',
  'Outkast', 'Manoj Muntashir', 'Vance Joy', 'Brent Faiyaz', 'The Notorious B.I.G.',
  'El Bogueto', 'Sade', 'BLACKPINK', 'BTS', 'Trippie Redd',
  'System Of A Down', 'Kapo', 'Jess Glynne', 'Evanescence', 'Central Cee',
  'Rema', 'G-Eazy', 'Lola Young', 'Megan Thee Stallion', 'Keyshia Cole',
  'Himesh Reshammiya', 'Kumaar', 'Lily-Rose Depp', 'Dire Straits', 'Tears For Fears',
  'AFROJACK', 'Steve Lacy', 'Fall Out Boy', 'Sayeed Quadri', 'Laufey',
  'Anderson .Paak', 'Rels B', 'Oscar Maydon', 'Shaggy', 'Kings of Leon',
  'Lord Huron', 'Avril Lavigne', 'The Cure', 'TV Girl', 'Yandel',
  'Yung Beef', 'Natanael Cano', 'B.o.B', 'Disco Lines', 'Dr. Dre',
  'Sebastian Yatra', 'Tory Lanez', 'She Y Him', 'T-Pain', 'MC Meno K',
  'Tyga', 'Shashwat Sachdev', 'Anirudh Ravichander', 'Mitski', 'Shilpa Rao',
  'Nelly', 'Eagles', 'Keane', 'Vishal Dadlani', 'beabadoobee',
  'Jack Harlow', 'Conan Gray', 'Elvis Presley', 'Gwen Stefani', 'Jowell Randy',
  'De La Soul', 'Panic! At The Disco', 'Madison Beer', 'Alok', 'Meghan Trainor',
  'The Smiths', 'benny blanco', 'Amit Trivedi', 'U2', 'Amy Winehouse',
  'Daryl Hall John Oates', 'Javed Ali', 'John Legend', 'Camilo', 'd4vd',
  'Maneskin', 'FloyyMenor', 'Limp Bizkit', 'Sachet-Parampara', 'Mc Gw',
  'Disclosure', 'Jelly Roll', 'Kate Bush', 'Marvin Gaye', 'JVKE',
  'Marc Anthony', 'Sajid-Wajid', 'HUGEL', 'Prince', 'Kunaal Vermaa',
  'Luis Miguel', 'Anu Malik', 'Grupo Firme', 'Labrinth', 'Fetty Wap',
  'Myles Smith', 'Sech', 'Foo Fighters', 'DJ Khaled', 'a-ha',
  'Earth Wind Fire', 'Luis R Conriquez', 'Lost Frequencies', 'F1 The Album', 'Mora',
  'Daya', 'Yo Yo Honey Singh', 'Neeti Mohan', 'Gabito Ballesteros', 'W Sound',
  'MC Ryan SP', 'Mac DeMarco', 'Martin Garrix', 'George Michael', 'Christian Nodal',
  'Eladio Carrion', 'Journey', 'Jubin Nautiyal', 'R.E.M.', 'The Offspring',
  'Bruce Springsteen', 'BigXthaPlug', 'Ella Langley', 'Natasha Bedingfield', 'Rahat Fateh Ali Khan',
  'Glass Animals', 'Bill Withers', 'EsDeeKid', 'Imogen Heap', 'Faheem Abdullah',
  'Sachet Tandon', 'Amaal Mallik', 'Parampara Tandon', 'Julieta Venegas', 'Justin Quiles',
  'Bryson Tiller', 'Skrillex', 'Fred again..', 'Lata Mangeshkar', 'YoungBoy Never Broke Again',
  'Lenny Tavarez', 'David Bowie', 'Reik', 'Led Zeppelin', '3 Doors Down',
  'Diljit Dosanjh', 'Coolio', 'Jonas Blue', 'Bastille', 'Nadhif Basalamah',
  'Lauv', 'Lil Peep', 'Train', 'Ray Dalton', 'My Chemical Romance',
  'Chase Atlantic', 'Bryant Myers', 'Baby Keem', 'Dido', 'De La Ghetto',
  'Macklemore Ryan Lewis', 'Stevie Wonder', 'S. P. Balasubrahmanyam', 'Young Miko', 'Big Sean',
  'Banda MS', 'Kailash Kher', 'Muse', 'Carly Rae Jepsen', 'Alejandro Sanz',
  '5 Seconds of Summer', 'Hotel Ugly', 'Noriel', 'Kelly Clarkson', 'Xavi',
  'Frank Sinatra', 'Lil Durk', 'Summer Walker', 'Stephen Sanchez', 'Jessie J',
  'Foreigner', 'Celine Dion', 'Juanes', 'Shaarib Toshi', 'Julion Alvarez',
  'Aditya Rikhari', 'Jason Mraz', 'Kausar Munir', 'GIMS', 'Karan Aujla',
  'blink-182', 'Ricky Martin', 'Marco Antonio Solis', 'Bob Sinclar', 'The 1975',
  'Migos', 'Devi Sri Prasad', 'Tulsi Kumar', 'Annie Lennox', 'Hariharan',
  'Jeremih', 'Felix Jaehn', 'L.V.', 'Troye Sivan', 'FISHER',
  'MGMT', 'Lil Tecca', 'DJ Japa NK', 'Mike Posner', 'Anuv Jain',
  'Roop Kumar Rathod', 'JID', 'Addison Rae', 'Shaboozey', 'Lynyrd Skynyrd',
  'Mc Rodrigo do CN', 'Yan Block', 'Yeat', 'Rochak Kohli', 'Scorpions',
  'Lil Nas X', 'Sexyy Red', 'Jeet Gannguli', 'Weezer', 'Calum Scott',
  'Rex Orange County', 'AP Dhillon', 'Roddy Ricch', 'Ayra Starr', 'Victor Mendivil',
  'A Boogie Wit da Hoodie', 'Bomba Estereo', 'Zedd', 'Alphaville', 'Silk Sonic',
  'Cyndi Lauper', 'Disturbed', 'Sukhwinder Singh', 'Three Days Grace', 'Deftones',
  'Offset', 'Mon Laferte', 'Badshah', 'The Lumineers', 'Alessia Cara',
  'Maria Becerra', 'slxughter', 'CeeLo Green', 'Chino Pacas', 'Stromae',
  'Black Sabbath', 'Tones And I', 'Grupo Menos Es Mais', 'Sameer Anjaan', 'Wale',
  'Darell', 'Mumford Sons', 'Boney M.', 'Joan Sebastian', 'Clairo',
  'Piso 21', 'Alejandro Fernandez', 'Milo j', 'Madhubanti Bagchi', 'TWICE',
  'Gym Class Heroes', 'Jonas Brothers', 'Christina Perri', 'Ajay-Atul', 'Jaani',
  'PEDRO SAMPAIO', 'Mc Jacare', 'Zion', 'Jasmine Sandlas', 'Lenny Kravitz',
  'Calibre 50', 'Benny Dayal', 'Andrew Choi', 'Latto', 'Pearl Jam',
  'Gotye', 'Rvssian', 'Idgitaf', 'Los Angeles Azules', 'Juan Luis Guerra',
  'Jatin-Lalit', 'La Arrolladora', 'Grupo Marca Registrada', 'Tony Dize', 'KISS',
  'Armaan Malik', 'Ice Spice', 'Paulo Londra', 'Yebba', 'Bob Dylan',
  'Billy Idol', 'mikeeysmind', 'Arcane', 'Dean Lewis', 'Alesso',
  'Little Mix', 'The Fray', 'Henrique Juliano', 'Ruth B', 'Bon Iver',
  'De La Rose', 'GloRilla', 'Lin-Manuel Miranda', 'Kelly Rowland', 'Topic',
  'Duki', 'Asha Bhosle', 'John Newman', 'TINI', 'The Temper Trap',
  'Gucci Mane', 'Wisin', 'Calle 24', 'NATTI NATASHA', 'Tove Lo',
  'Roxette', 'ZXKAI', 'Chuwi', 'Mary J. Blige', 'Electric Light Orchestra',
  'bees honey', 'Jonita Gandhi', 'John Mayer', 'Carlos Vives', 'Kumar Sanu',
  'Salim-Sulaiman', 'Aleman', 'Melanie Martinez', 'WILLOW', 'Ben E. King',
  'MXZI', 'League of Legends', 'Dave Stewart', 'K. S. Chithra', 'Sting',
  'Gente De Zona', 'DJ Luian', 'Kacey Musgraves', 'Gabry Ponte', 'Eslabon Armado',
  'Palak Muchhal', 'WizTheMc', 'will.i.am', 'Slipknot', 'Kimbra',
  'Dolly Parton', 'Shania Twain', 'T.I.', 'Los Pleneros de la Cresta', 'Rae Sremmurd',
  'Danger Mouse', 'Tracy Chapman', 'Eurythmics', 'Sadhana Sargam', 'Jasleen Royal',
  'Gesaffelstein', 'Ariis', 'Jesse Joy', 'The Strokes', 'Destinys Child',
  'Sid Sriram', 'Neha Kakkar', 'CYRIL', 'Hector El Father', 'The Clash',
  'Ice Cube', 'Saja Boys', 'Cosculluela', 'Armaan Khan', 'Anand Raj Anand',
  'Pop Smoke', 'Skepta', 'Jung Kook', 'The Pussycat Dolls', 'Eden Munoz',
  'DJ DAVI DOGDOG', 'Beach House', 'Nile Rodgers', 'Luar La L', 'Hindia',
  'TLC', 'Lenin Ramirez', 'B Praak', 'Elley Duhe', 'Artemas',
  'NIKI', 'PNAU', 'Pamungkas', 'Julia Michaels', 'Mambo Kingz',
  'LE SSERAFIM', 'Los Dareyes De La Sierra', 'Guru Randhawa', 'NF', 'Rick Ross',
  'Mc Jhey', 'Ikky', 'NLE Choppa', 'Chayanne', 'Mazzy Star',
  'Ozzy Osbourne', 'Lil Yachty', 'Shaan', 'Olly Alexander', 'Nicki Nicole',
  'Willie Colon', 'Ms. Lauryn Hill', 'Jorge Mateus', 'Santana', 'AURORA',
  'Starship', 'Lykke Li', 'blackbear', 'Iyaz', 'Noah Cyrus',
  'Blondie', 'girl in red', 'Alfredo Olivas', 'Yorghaki', 'America',
  'Creed', 'Alleh', 'Morat', 'Jay Wheeler', 'mgk',
  'Leon Thomas', 'Polo G', 'ROA', 'Yahritza Y Su Esencia', 'Sayfalse',
  'M8', 'Cage The Elephant', 'Harris Jayaraj', 'Depeche Mode', 'Rashmi Virag',
  'KEVIN WOO', 'Danny Chung', 'samUIL Lee', 'Neckwav', 'Malcolm Todd',
  'The Verve', 'Lizzy McAlpine', 'Gajendra Verma', 'Vicente Fernandez', 'Melody',
  'Juan Gabriel', 'Dimitri Vegas Like Mike', 'LISA', 'Fifth Harmony', 'R. D. Burman',
  'Tito El Bambino', 'Herencia De Grandes', 'Mc Lele JP', 'Abhijeet', 'Duran Duran',
  'Lukas Graham', 'Trueno', 'Foster The People', 'Crowded House', 'Soulja Boy',
  'Dj Samir', 'Milky Chance', 'Yuvan Shankar Raja', 'Audioslave', 'The Game',
  'Icona Pop', 'Papa Roach', 'MARINA', 'ILLIT', 'Nadeem Shravan',
  'The Cardigans', 'Manu Chao', 'Gusttavo Lima', 'Dimitri Vegas', 'Darshan Raval',
  'Niall Horan', 'Emilia', 'Cassie', 'Mc Don Juan', 'Asees Kaur',
  'Steve Aoki', 'The White Stripes', 'Matheus Kauan', 'Alec Benjamin', 'Raim Laode',
  'Robbie Williams', 'Kris R.', 'bbno$', 'Gulzar', 'Mc Livinho',
  'Kylie Minogue', 'Armin van Buuren', 'Karthik', 'Wizkid', 'La Oreja de Van Gogh',
  'The Walters', 'Divya Kumar', 'Los Enanitos Verdes', 'Bonnie Tyler', 'Talwiinder',
  'Michael Buble', 'Fergie', 'Bradley Cooper', 'Tiago PZK', 'Bring Me The Horizon',
  'Omar Camacho', 'Chief Keef', 'Kavita Krishnamurthy', 'Mahalakshmi Iyer', 'Ana Castela',
  'Survivor', 'Selena Gomez The Scene', 'ELENA ROSE', 'WALK THE MOON', 'The Smashing Pumpkins',
  'Taio Cruz', 'fun.', 'Kishore Kumar', 'Sufjan Stevens', 'Ace of Base',
  'Gangsta', 'NewJeans', 'RaiNao', 'Waka Flocka Flame', 'Shekhar Ravjiani',
  'Los Tigres Del Norte', 'Meek Mill', 'Brytiago', 'No Doubt', 'Jory Boy',
  'Rico Ace', 'Chrystal', 'Chris Isaak', 'Seeb', 'MAGIC!',
  'Zendaya', 'Haze', 'Nate Ruess', 'COLORS', 'Rammstein',
  'Eric Clapton', 'Sheila On 7', 'WW', 'Indila', 'Portugal. The Man',
  'Edgardo Nunez', 'King Von', 'Simone Mendes', 'Hanumankind', 'Soda Stereo',
  'Haddaway', 'Gnarls Barkley', 'Flo Milli', 'Dalex', 'Raj Shekhar',
  'John Lennon', 'James Blunt', 'Tego Calderon', 'Hans Zimmer', 'Surf Curse',
  'Pablo Alboran', '6ix9ine', 'Jose Jose', 'Tulus', 'Boza',
  'Aitana', 'Arash', 'Thaman S', 'Paul McCartney', 'MC Nito',
  'Los Tucanes De Tijuana', 'Bad Gyal', 'G. V. Prakash', 'Modern Talking', 'Hoobastank',
  'Laura Pausini', 'New West', 'Lunay', 'Cher', 'Rod Stewart',
  'NATTAN', 'Riley Green', 'Ankit Tiwari', 'MC Tuto', 'Oruam',
  'Leon Bridges', 'Shafqat Amanat Ali', 'Cazzu', 'Busta Rhymes', '6LACK',
  'Sal Priadi', 'Van Halen', 'Sublime', 'NSYNC', 'Frankie Valli',
  'KALEO', 'OMI', 'KHEA', 'Jessie Murph', 'Jack Johnson',
  'Jaideep Sahni', 'Diego Victor Hugo', 'Tokischa', 'Mario', 'Korn',
  'Em Beihold', 'Morad', 'Randy Nota Loca', 'Spice Girls', 'Panda',
  'Thomas Rhett', 'Virlan Garcia', 'ATL Jacob', 'Nikhita Gandhi', 'Kushagra',
  'Olivia Newton-John', 'The Stranglers', '070 Shake', 'MNEK', 'Johnny Cash',
  'Murilo Huff', 'Bailey Zimmerman', 'Jhené Aiko', 'MC GP', 'Snow Patrol',
  'The Ronettes', 'LUDMILLA', 'Nio Garcia', 'The Animals', 'Nadin Amizah',
  'ATLXS', 'Sixpence None The Richer', 'John Summit', '4 Non Blondes', 'Shubh',
  'Thundercat', 'Galantis', 'j-hope', 'Clave Especial', 'Javed Akhtar',
  'Altamash Faridi', 'Cristian Castro', 'Phoebe Bridgers', 'Quavo', 'Rekha Bhardwaj',
  'MC IG', 'Jawad Ahmad', 'Skillet', 'MEDUZA', 'Ciara',
  'Nakama', 'Ilaiyaraaja', 'ILLENIUM', 'boa', 'DFZM',
  'Shashaa Tirupati', 'Luan Santana', 'Saaheal', 'David Kushner', 'Kim Petras',
  'Young Cister', 'Haricharan', 'Micro TDH', 'RUFUS DU SOL', 'Kany Garcia',
  'Jeff Buckley', 'Chuyin', 'Santa Fe Klan', 'John Martin', 'Normani',
  'Nakash Aziz', 'Willy William', 'Adam Port', 't.A.T.u', 'Cardenales De Nuevo Leon',
  'Gael Valenzuela', 'Camila', 'Hombres G', 'Jay Sean', 'Marc Segui',
  'Clementine Douglas', 'NOTION', 'Tammi Terrell', 'Nanpa Basico', 'Leo Foguete',
  'Avenged Sevenfold', 'Omah Lay', 'Alice In Chains', 'Daecolm', 'Ram Sampath',
  'The Outfield', 'LMFAO', 'Natalia Lafourcade', 'SYML', 'MOLIY',
  'Lil Tjay', 'Shenseea', 'Brooks Dunn', 'Chicago', 'Bharath',
  'Tommy Richman', 'Gigi DAgostino', 'Lionel Richie', 'Cup of Joe', 'MO',
  'Zé Neto Cristiano', 'Men At Work', 'La Factoria', 'Passenger', 'Juan Magan',
  'oskar med k', 'Pol Granch', 'Harshdeep Kaur', 'Anuradha Paudwal', 'Rudimental',
  'James Hype', 'Vijay Prakash', 'Maren Morris', 'Montell Fish', 'Sai Abhyankkar',
  'Priya Saraiya', 'Kenny Rogers', 'Eddy Lover', 'Counting Crows', 'Cartel De Santa',
  'Luny Tunes', 'Mc Negão Original', 'Cody Johnson', 'Diana Ross', 'R3HAB',
  'Naresh Iyer', 'Strawberry Guy', 'Sam Fender', '.Feast', 'Laxmikant-Pyarelal',
  'MC Jvila', 'Ian Asher', 'INNA', 'Clams Casino', 'Neeraj Shridhar',
  'Jason Aldean', 'Banda El Recodo', 'Keinemusik', 'Lily Allen', 'Moneybagg Yo',
  'Hades66', 'Phantogram', 'Manan Bhardwaj', 'Miranda!', 'C. Tangana',
  'JC Reyes', 'LANY', 'YUNGBLUD', 'Felipe Amorim', 'MJ Records',
  'The Beach Boys', 'Masoom Sharma', 'Wham!', 'Skillibeng', 'Jorja Smith',
  'Nusrat Fateh Ali Khan', 'Chezile', 'Eyedress', 'George Ezra', 'DNCE',
  'The Mamas The Papas', 'Russ', 'Aretha Franklin', 'Edward Maya', 'Pet Shop Boys',
  'Antara Mitra', 'Belinda', 'SAIKO', 'Stray Kids', 'Sohail Sen',
  'Jere Klein', 'Chris Grey', 'Tina Turner', 'Patrick Watson', 'MC LUUKY',
  'Bappi Lahiri', 'Meet Bros.', 'Lloyd', '$uicideboy$', 'DMX',
  'Axwell', 'Siddharth Garima', 'DJ Oreia', 'Mika Singh', 'DENNIS',
  'Flume', 'Dhanush', 'Lyodra', 'Chris Jedi', 'Turma do Pagode',
  'Tyler Childers', 'Hugo Guilherme', 'Shweta Mohan', 'Dermot Kennedy', 'Richy Mitch',
  'Zac Efron', 'The Doors', 'Malachiii', 'Javed Bashir', 'For Revenge',
  'Blur', 'Selena', 'Birdy', 'Disciples', 'The All-American Rejects',
  'Gerardo Ortiz', 'Arslan Nizami', 'Brray', 'Arko', 'Jasiel Nunez',
  'Jon', 'Rascal Flatts', 'Zoé', 'Naresh Kamath', 'Luke Bryan',
  'Dhanda Nyoliwala', 'Marilia Mendonca', 'Omar Apollo', 'Franz Ferdinand', 'Megan Moroney',
  'Leona Lewis', 'Zé Felipe', 'Rage Against The Machine', 'Ray Charles', 'Casper Magico',
  'Timmy Trumpet', 'Bleachers', 'Modjo', 'Paresh Kamath', 'Jon Alvarez',
  'Jon Pardi', 'Motley Crue', 'Bazzi', 'Majid Jordan', 'MK',
  'Frankie Ruiz', 'Nejo', 'Luisa Sonza', 'DJ Javi26', 'Kenshi Yonezu',
  '24kGoldn', 'Kungs', 'Natanzinho Lima', 'YNW Melly', 'Owl City',
  'NDS', 'Sevdaliza', 'D-Block Europe', 'A$AP Ferg', 'YS',
  'Lyanno', 'Heart', 'Elevation Worship', 'Lady A', 'Virgoun',
  'Miguel Bose', 'NAV', 'The Hollies', 'Stryv', 'Kansas',
  'Of Monsters and Men', 'Emmanuel Cortes', 'Chord Overstreet', 'Zevia', 'Foushee',
  'Rich Brian', 'Coi Leray', 'Giveon', 'Yung Gravy', 'Stephen Sanchez',
  'Angus Julia Stone', 'Ruel', 'Clairo', 'Wallows', 'Owl City',
  'Phoebe Buffay', 'A Great Big World', 'Tash Sultana', 'Girl in Red',
  'Montaigne', 'Baker Boy', 'Pentatonix', 'Cimorelli', 'Sami Yusuf',
  'Maher Zain', 'Mohamed Hamaki', 'Amr Diab', 'Tamer Hosny', 'Mohamed Ramadan',
  'Wegz', 'Marwan Moussa', 'Moe Shop', 'Ado', 'YOASOBI',
  'Iruma', 'Radwimps', 'KANA-BOON', 'LiSA', 'Official HIGE DANdism',
  'Mrs. GREEN APPLE', 'SEVENTEEN', 'IVE', 'NMIXX', 'aespa',
  'NCT', 'ENHYPEN', 'TREASURE', 'Red Velvet', 'ITZY',
  'GI-DLE', 'LOONA', 'APink', 'GFRIEND', 'BoA',
  'TVXQ', 'Super Junior', 'SHINee', 'EXO', 'GOT7',
  'MONSTA X', 'TXT', 'ATEEZ', 'Joel Corry', 'Riton',
];

const MAX_DEPTH = 3;
const DEPTH2_SPLITS = 3;
const DEPTH3_SPLITS_PER_DEPTH2 = 3;

const ARTIST_WORKERS = 12;
const MAX_BROWSERS = 10;
const PARALLEL_PER_BROWSER = 5;
const TOTAL_TASK_LOOPS = MAX_BROWSERS * PARALLEL_PER_BROWSER;

const MAX_QUEUE_SIZE = 30000;
const MAX_TRACKS_PER_ITEM = 500;
const MAX_SUBPAGES_PER_ARTIST = 3;
const PAGE_TIMEOUT = 30000;
const DELAY_BETWEEN_PAGES = 120;
const HEARTBEAT_MS = 30000;

const ROOT_DIR = __dirname;
const DATA_DIR = path.join(ROOT_DIR, 'data');
const WORKFLOW_DIR = path.join(DATA_DIR, 'workflows');
const OUTPUT_DIR = path.join(DATA_DIR, 'outputs');
const LOG_DIR = path.join(DATA_DIR, 'logs');

const args = process.argv.slice(2);
const MODE = getArg('--mode') || 'orchestrator';
const WORKFLOW_FILE = getArg('--workflow') || null;
const FINAL_OUTPUT = getArg('--out') || path.join(DATA_DIR, 'us.json');
const MERGE_GLOB = getArg('--merge-glob') || path.join(OUTPUT_DIR, '*.json');

function getArg(flag) {
  const i = args.findIndex(a => a === flag);
  return i >= 0 ? (args[i + 1] || null) : null;
}
function ensureDir(d) { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); }
function ensureDataDirs() { [DATA_DIR, WORKFLOW_DIR, OUTPUT_DIR, LOG_DIR].forEach(ensureDir); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function generateId(url) {
  const hash = url.split('').reduce((a, b) => {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
  return Math.abs(hash).toString(36);
}
function saveJson(file, data) { fs.writeFileSync(file, JSON.stringify(data, null, 2)); }
function loadJson(file, fallback = null) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {}
  return fallback;
}
function detectType(url = '') {
  const u = url.toLowerCase();
  if (u.includes('/artist/')) return 'artist';
  if (u.includes('/song/')) return 'song';
  if (u.includes('/album/')) return 'album';
  if (u.includes('/single/') || u.includes('/ep/')) return 'single';
  if (u.includes('/playlist/')) return 'playlist';
  if (u.includes('/chart/')) return 'chart';
  if (u.includes('/radio') || u.includes('/station/')) return 'radio';
  if (u.includes('/room/')) return 'room';
  return 'other';
}
function splitArray(arr, parts) {
  const out = Array.from({ length: parts }, () => []);
  for (let i = 0; i < arr.length; i++) out[i % parts].push(arr[i]);
  return out;
}
function dedupeByUrl(items) {
  const out = [];
  const seen = new Set();
  for (const x of items || []) {
    if (!x?.url || seen.has(x.url)) continue;
    seen.add(x.url);
    out.push(x);
  }
  return out;
}
function normalizeOutputItem(item) {
  return {
    id: generateId(item.url),
    name: item.name || 'Unknown',
    type: item.type || detectType(item.url),
    country: 'us',
    url: item.url,
    searchTerms: (item.name || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').trim(),
    scrapedAt: new Date().toISOString(),
    creator: item.creator || '',
    metadata: item.metadata || '',
    description: item.description || '',
    tracks: (item.tracks || []).slice(0, MAX_TRACKS_PER_ITEM),
    sections: item.sections || [],
    featuredItems: item.featuredItems || [],
    subItems: item.subItems || [],
  };
}

function startHeartbeat(label, getSnapshot) {
  const timer = setInterval(() => {
    try {
      const snap = getSnapshot ? getSnapshot() : {};
      console.log(`[HEARTBEAT:${label}] ${new Date().toISOString()} ${JSON.stringify(snap)}`);
    } catch (e) {
      console.log(`[HEARTBEAT:${label}] ${new Date().toISOString()} alive`);
    }
  }, HEARTBEAT_MS);
  return () => clearInterval(timer);
}

async function scrollUntilExhausted(page, direction = 'vertical') {
  await page.evaluate(async (dir) => {
    const delay = 700;
    const max = 50;
    let last = dir === 'vertical' ? window.scrollY : window.scrollX;
    let same = 0;
    for (let i = 0; i < max; i++) {
      if (dir === 'vertical') window.scrollBy(0, 900);
      else {
        const containers = document.querySelectorAll('[style*="overflow-x"], .shelf-grid, [class*="carousel"]');
        for (const c of containers) c.scrollBy({ left: 450, behavior: 'smooth' });
        window.scrollBy({ left: 450, top: 0, behavior: 'smooth' });
      }
      await new Promise(r => setTimeout(r, delay));
      const cur = dir === 'vertical' ? window.scrollY : window.scrollX;
      if (cur === last) {
        same++;
        if (same >= 3) break;
      } else same = 0;
      last = cur;
    }
    if (dir === 'vertical') window.scrollTo(0, 0);
  }, direction);
}

async function extractLinksSectionsAndTracks(page) {
  return page.evaluate(() => {
    const r = {
      links: [],
      sections: [],
      tracks: [],
      featuredItems: [],
      pageTitle: '',
      pageSubtitle: '',
      pageMetadata: '',
      pageDescription: '',
    };

    function typeOf(url = '') {
      const u = url.toLowerCase();
      if (u.includes('/artist/')) return 'artist';
      if (u.includes('/song/')) return 'song';
      if (u.includes('/album/')) return 'album';
      if (u.includes('/single/') || u.includes('/ep/')) return 'single';
      if (u.includes('/playlist/')) return 'playlist';
      if (u.includes('/chart/')) return 'chart';
      if (u.includes('/radio') || u.includes('/station/')) return 'radio';
      if (u.includes('/room/')) return 'room';
      return 'other';
    }

    r.pageTitle = document.querySelector('[class*="headings__title"]')?.textContent?.trim() || '';
    r.pageSubtitle = document.querySelector('[class*="headings__subtitles"]')?.textContent?.trim() || '';
    r.pageMetadata = document.querySelector('[class*="headings__metadata-bottom"]')?.textContent?.trim() || '';
    r.pageDescription = document.querySelector('[class*="description"]')?.textContent?.trim() || '';

    const anchors = document.querySelectorAll('a[href]');
    for (const a of anchors) {
      if (!a.href?.includes('music.apple.com/us/')) continue;
      const u = a.href.split('?')[0].split('#')[0];
      if (!r.links.includes(u)) r.links.push(u);
    }

    const sectionEls = document.querySelectorAll('[data-testid="section-container"]');
    for (const sec of sectionEls) {
      const name = sec.querySelector('h2, h3, [class*="title"]')?.textContent?.trim() || 'Section';
      const items = [];
      const itemLinks = sec.querySelectorAll('a[href*="/album/"],a[href*="/playlist/"],a[href*="/artist/"],a[href*="/chart/"]');
      for (const l of itemLinks) {
        const u = l.href?.split('?')[0]?.split('#')[0];
        if (!u || !u.includes('music.apple.com')) continue;
        items.push({ name: (l.textContent || '').trim().substring(0, 200) || u, url: u, type: typeOf(u) });
      }
      if (items.length) r.sections.push({ name, items: dedupe(items) });
    }

    const featSections = document.querySelectorAll('[data-testid="section-container"][aria-label="Featured"]');
    for (const sec of featSections) {
      const cards = sec.querySelectorAll('[class*="lockup"], a[href*="/playlist/"], a[href*="/album/"], a[href*="/chart/"], a[href*="/room/"]');
      for (const c of cards) {
        const linkEl = c.tagName === 'A' ? c : c.querySelector('a[href]');
        const raw = linkEl?.href || '';
        if (!raw.includes('music.apple.com')) continue;
        const url = raw.split('?')[0].split('#')[0];
        const title = c.querySelector('[class*="headings__title"], [class*="title"]')?.textContent?.trim() || '';
        const subtitle = c.querySelector('[class*="headings__subtitles"]')?.textContent?.trim() || '';
        const metadata = c.querySelector('[class*="headings__metadata-bottom"]')?.textContent?.trim() || '';
        const description = c.querySelector('[class*="description"]')?.textContent?.trim() || '';
        if (title && url) r.featuredItems.push({ name: title, url, type: typeOf(url), creator: subtitle, metadata, description });
      }
    }

    const selectors = ['div.songs-list-row', 'li.songs-list-item', '[data-testid="track-row"]', 'div[class*="track"]'];
    let trackEls = [];
    for (const s of selectors) {
      const f = document.querySelectorAll(s);
      if (f.length) { trackEls = Array.from(f); break; }
    }
    if (!trackEls.length) trackEls = Array.from(document.querySelectorAll('a[href*="/song/"]'));

    const seen = new Set();
    trackEls.forEach((el, idx) => {
      let url = '';
      if (el.href?.includes('/song/')) url = el.href.split('?')[0];
      else {
        const sl = el.querySelector('a[href*="/song/"]');
        if (sl) url = sl.href.split('?')[0];
      }
      if (!url || seen.has(url)) return;
      seen.add(url);

      const name = (el.querySelector('[class*="title"],[class*="name"]')?.textContent?.trim() || el.textContent?.trim() || '').substring(0, 200);
      if (!name) return;

      r.tracks.push({
        name,
        url,
        artist: el.querySelector('[class*="artist"], .by-line')?.textContent?.trim() || '',
        album: el.querySelector('[class*="album"]')?.textContent?.trim() || '',
        duration: el.querySelector('[class*="duration"], .time')?.textContent?.trim() || '',
        position: idx + 1,
      });
    });

    function dedupe(a) {
      const o = [];
      const s = new Set();
      for (const x of a) {
        if (!x?.url || s.has(x.url)) continue;
        s.add(x.url); o.push(x);
      }
      return o;
    }

    return r;
  });
}

async function crawlPage(page, url) {
  try {
    await page.goto(url, { waitUntil: 'networkidle', timeout: PAGE_TIMEOUT });
    await scrollUntilExhausted(page, 'vertical');
    await scrollUntilExhausted(page, 'horizontal');
    return await extractLinksSectionsAndTracks(page);
  } catch (e) {
    console.error(`Error crawling ${url}: ${e.message}`);
    return {
      links: [], sections: [], tracks: [], featuredItems: [],
      pageTitle: '', pageSubtitle: '', pageMetadata: '', pageDescription: '',
    };
  }
}

async function createBrowserPool() {
  const list = [];
  for (let i = 0; i < MAX_BROWSERS; i++) {
    list.push(await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] }));
  }
  return list;
}
async function closeBrowserPool(pool) {
  for (const b of pool) {
    try { await b.close(); } catch {}
  }
}

async function processPageTask(task, browser, state) {
  const { url, depth } = task;
  if (!url || state.visited.has(url)) return { processed: false, links: [], tracksCount: 0, subtitle: '', depth };
  state.visited.add(url);

  const t = detectType(url);
  if (t === 'song' || t === 'radio') return { processed: false, links: [], tracksCount: 0, subtitle: '', depth };

  const page = await browser.newPage();
  try {
    const x = await crawlPage(page, url);
    const isAppleMusicPage = (x.pageSubtitle || '').toLowerCase().includes('apple music');
    const isAllowed = depth === 1 || isAppleMusicPage;

    if (isAllowed && (x.tracks.length || x.featuredItems.length)) {
      state.itemsByUrl.set(url, normalizeOutputItem({
        name: x.pageTitle || url.split('/').pop() || 'Unknown',
        url,
        type: t,
        creator: x.pageSubtitle || 'Apple Music',
        metadata: x.pageMetadata || '',
        description: x.pageDescription || '',
        tracks: x.tracks || [],
        sections: x.sections || [],
        featuredItems: x.featuredItems || [],
      }));
    }

    const newLinks = [];
    if (depth < MAX_DEPTH) {
      for (const l of x.links || []) {
        if (state.visited.has(l)) continue;
        const lt = detectType(l);
        if (lt === 'song' || lt === 'radio') continue;
        newLinks.push({ url: l, depth: depth + 1 });
      }
    }

    return {
      processed: true,
      links: dedupeByUrl(newLinks),
      tracksCount: (x.tracks || []).length,
      subtitle: x.pageSubtitle || '',
      depth,
    };
  } finally {
    await page.close();
  }
}

async function runQueueWithPool(initialQueue, state, browsers, workerLabel) {
  const queue = [...initialQueue];
  let pageCount = 0;

  let queueLock = Promise.resolve();
  async function takeTask() {
    let task = null;
    await (queueLock = queueLock.then(() => {
      if (queue.length > MAX_QUEUE_SIZE) queue.length = MAX_QUEUE_SIZE;
      task = queue.shift() || null;
    }));
    return task;
  }

  let metricLock = Promise.resolve();
  let batchProcessed = 0;
  let batchTracks = 0;
  let batchDepth = null;
  const batchSubtitles = new Set();

  const stopHeartbeat = startHeartbeat(`crawl:${workerLabel}`, () => ({
    pageCount,
    queue: queue.length,
    maxQueue: MAX_QUEUE_SIZE,
    items: state.itemsByUrl.size,
    batchProcessed,
    batchTracks,
    batchDepth: batchDepth ?? null,
  }));

  async function addMetrics(result) {
    await (metricLock = metricLock.then(() => {
      if (batchDepth === null && typeof result.depth === 'number') batchDepth = result.depth;
      batchProcessed += result.processed ? 1 : 0;
      batchTracks += result.tracksCount || 0;
      if (result.subtitle) batchSubtitles.add(result.subtitle);
    }));
  }

  async function flushMetrics(force = false) {
    await (metricLock = metricLock.then(() => {
      if (!force && batchProcessed < 10) return;
      if (batchProcessed === 0) return;

      const items = Array.from(state.itemsByUrl.values());
      const allTotalTracks = items.reduce((sum, item) => sum + (item.tracks?.length || 0), 0);
      const subtitles = [...batchSubtitles].join(', ') || 'N/A';
      const depthForLog = batchDepth ?? '?';

      console.log(
        `Pages Crawled: ${pageCount} | Depth: ${depthForLog}/${MAX_DEPTH} | Processed Pages: ${batchProcessed} | Total Processed Pages: ${pageCount} | Queue: ${queue.length}/${MAX_QUEUE_SIZE} | Items: ${items.length} | Tracks from Batch: ${batchTracks} | All Total Tracks: ${allTotalTracks} | Subtitle of Batch: ${subtitles}`
      );

      batchProcessed = 0;
      batchTracks = 0;
      batchDepth = null;
      batchSubtitles.clear();
    }));
  }

  async function loop(browserIndex) {
    const browser = browsers[browserIndex];
    while (true) {
      const task = await takeTask();
      if (!task) break;

      try {
        const r = await processPageTask(task, browser, state);
        if (r.processed) pageCount += 1;

        for (const nl of r.links || []) {
          if (!state.visited.has(nl.url) && queue.length < MAX_QUEUE_SIZE) queue.push(nl);
        }

        await addMetrics(r);
        if (pageCount > 0 && pageCount % 10 === 0) {
          await flushMetrics(false);
        }
      } catch (e) {
        console.error(`[${workerLabel}] task error: ${e.message}`);
      }
      await sleep(DELAY_BETWEEN_PAGES);
    }
  }

  const loops = [];
  for (let b = 0; b < MAX_BROWSERS; b++) {
    for (let p = 0; p < PARALLEL_PER_BROWSER; p++) loops.push(loop(b));
  }
  await Promise.all(loops);

  await flushMetrics(true);
  stopHeartbeat();
}

async function findArtistUrl(browser, artistName) {
  const page = await browser.newPage();
  try {
    await page.goto(`https://music.apple.com/us/search?term=${encodeURIComponent(artistName)}`, { waitUntil: 'networkidle', timeout: PAGE_TIMEOUT });
    await scrollUntilExhausted(page, 'vertical');
    return await page.evaluate((name) => {
      const links = document.querySelectorAll('a[href]');
      for (const l of links) {
        const href = l.href;
        const txt = (l.textContent || '').trim();
        if (href?.includes('/artist/') && txt.toLowerCase() === name.toLowerCase()) return href.split('?')[0];
      }
      const first = Array.from(links).find(l => l.href?.includes('/artist/'));
      return first ? first.href.split('?')[0] : null;
    }, artistName);
  } finally {
    await page.close();
  }
}

async function processArtistPage(browser, artistName, artistUrl, visited) {
  const page = await browser.newPage();
  const artistData = {
    name: artistName, url: artistUrl, type: 'artist', creator: artistName,
    metadata: '', description: '', tracks: [], sections: [], featuredItems: [], subItems: [],
  };
  try {
    await page.goto(artistUrl, { waitUntil: 'networkidle', timeout: PAGE_TIMEOUT });
    await scrollUntilExhausted(page, 'vertical');
    await scrollUntilExhausted(page, 'horizontal');

    const x = await extractLinksSectionsAndTracks(page);
    artistData.sections = x.sections || [];
    artistData.featuredItems = x.featuredItems || [];
    artistData.metadata = x.pageMetadata || '';
    artistData.description = x.pageDescription || '';

    const subUrls = new Set();
    for (const sec of artistData.sections) {
      for (const it of sec.items || []) {
        const t = detectType(it.url);
        if (['album', 'single', 'playlist'].includes(t)) {
          artistData.subItems.push({ name: it.name, url: it.url, type: t, tracks: [] });
          if (!visited.has(it.url)) subUrls.add(it.url);
        }
      }
    }

    const toProcess = Array.from(subUrls).slice(0, MAX_SUBPAGES_PER_ARTIST);
    for (const subUrl of toProcess) {
      const sp = await browser.newPage();
      try {
        await sp.goto(subUrl, { waitUntil: 'networkidle', timeout: PAGE_TIMEOUT });
        await scrollUntilExhausted(sp, 'vertical');
        const sd = await extractLinksSectionsAndTracks(sp);
        const target = artistData.subItems.find(s => s.url === subUrl);
        if (target) target.tracks = (sd.tracks || []).slice(0, MAX_TRACKS_PER_ITEM);
        visited.add(subUrl);
      } catch (e) {
        console.error(`subpage error ${subUrl}: ${e.message}`);
      } finally {
        await sp.close();
      }
    }

    return artistData;
  } finally {
    await page.close();
  }
}

function workflowPath(name) { return path.join(WORKFLOW_DIR, name); }
function outputPath(name) { return path.join(OUTPUT_DIR, name); }

function writeWorkflow(name, payload) {
  const file = workflowPath(name);
  saveJson(file, payload);
  return file;
}
function writeOutput(name, items, meta = {}) {
  const unique = dedupeByUrl(items);
  const normalized = unique.map(normalizeOutputItem);
  const payload = {
    lastUpdated: new Date().toISOString(),
    country: 'us',
    totalItems: normalized.length,
    totalTracks: normalized.reduce((s, i) => s + (i.tracks?.length || 0), 0),
    items: normalized,
    ...meta,
  };
  const file = outputPath(name);
  saveJson(file, payload);
  return file;
}

async function runWorker(workflowFile) {
  ensureDataDirs();
  if (!workflowFile || !fs.existsSync(workflowFile)) throw new Error(`Workflow missing: ${workflowFile}`);
  const wf = loadJson(workflowFile, null);
  if (!wf) throw new Error(`Invalid workflow: ${workflowFile}`);

  const workerLabel = wf.workerId || path.basename(workflowFile);
  const state = { visited: new Set(wf.visited || []), itemsByUrl: new Map() };
  for (const si of wf.seedItems || []) if (si?.url) state.itemsByUrl.set(si.url, normalizeOutputItem(si));

  const stopHeartbeat = startHeartbeat(`worker:${workerLabel}`, () => ({
    kind: wf.kind,
    depth: wf.depth || null,
    items: state.itemsByUrl.size,
    visited: state.visited.size,
  }));

  const pool = await createBrowserPool();
  try {
    if (wf.kind === 'crawl') {
      await runQueueWithPool(wf.queue || [], state, pool, workerLabel);

      if (wf.includeMandatoryArtists) {
        const b = pool[0];
        for (const a of MANDATORY_ARTISTS) {
          const ad = await processArtistPage(b, a.name, a.url, state.visited);
          state.itemsByUrl.set(ad.url, normalizeOutputItem(ad));
        }
      }
    } else if (wf.kind === 'artist-batch') {
      const artistQueue = [...(wf.artists || [])];
      const stopArtistHeartbeat = startHeartbeat(`artists:${workerLabel}`, () => ({
        remainingArtists: artistQueue.length,
        items: state.itemsByUrl.size,
        visited: state.visited.size,
      }));

      async function artistLoop(browserIndex) {
        const b = pool[browserIndex];
        while (true) {
          const artistName = artistQueue.shift();
          if (!artistName) break;
          try {
            const artistUrl = await findArtistUrl(b, artistName);
            if (!artistUrl) continue;
            const ad = await processArtistPage(b, artistName, artistUrl, state.visited);
            state.itemsByUrl.set(ad.url, normalizeOutputItem(ad));
          } catch (e) {
            console.error(`[${workerLabel}] artist ${artistName} error: ${e.message}`);
          }
        }
      }

      const loops = [];
      for (let b = 0; b < MAX_BROWSERS; b++) {
        for (let p = 0; p < PARALLEL_PER_BROWSER; p++) loops.push(artistLoop(b));
      }
      await Promise.all(loops);
      stopArtistHeartbeat();
    }
  } finally {
    await closeBrowserPool(pool);
    stopHeartbeat();
  }

  const items = Array.from(state.itemsByUrl.values());
  const outFile = writeOutput(wf.outputName, items, {
    phase: wf.phase || 'worker',
    workerId: workerLabel,
    kind: wf.kind,
    depth: wf.depth || null,
  });

  if (wf.emitNextDepthLinksFile) {
    const next = [];
    for (const it of items) {
      for (const sec of it.sections || []) {
        for (const si of sec.items || []) {
          const t = detectType(si.url);
          if (t !== 'song' && t !== 'radio') next.push({ url: si.url, depth: (wf.depth || 1) + 1 });
        }
      }
      for (const fi of it.featuredItems || []) {
        const t = detectType(fi.url);
        if (t !== 'song' && t !== 'radio') next.push({ url: fi.url, depth: (wf.depth || 1) + 1 });
      }
    }
    saveJson(wf.emitNextDepthLinksFile, { generatedAt: new Date().toISOString(), from: workerLabel, links: dedupeByUrl(next) });
  }

  console.log(`[${workerLabel}] done -> ${outFile}`);
  return outFile;
}

function spawnNode(childArgs, logFile) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [__filename, ...childArgs], {
      cwd: ROOT_DIR,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
    });

    const log = fs.createWriteStream(logFile, { flags: 'a' });

    child.stdout.on('data', (d) => {
      const s = d.toString();
      process.stdout.write(s);
      log.write(s);
    });

    child.stderr.on('data', (d) => {
      const s = d.toString();
      process.stderr.write(s);
      log.write(s);
    });

    child.on('error', (err) => {
      log.end();
      reject(err);
    });

    child.on('close', (code) => {
      log.end();
      if (code === 0) resolve();
      else reject(new Error(`Child failed (${code}): ${childArgs.join(' ')}`));
    });
  });
}

async function runMerge(inputGlob, outFile) {
  ensureDataDirs();
  const files = glob.sync(inputGlob).filter(f => path.resolve(f) !== path.resolve(outFile));
  const byUrl = new Map();

  for (const f of files) {
    const d = loadJson(f, null);
    if (!d?.items) continue;
    for (const it of d.items) {
      if (!it?.url) continue;
      const n = normalizeOutputItem(it);
      if (!byUrl.has(n.url)) byUrl.set(n.url, n);
      else {
        const cur = byUrl.get(n.url);
        byUrl.set(n.url, {
          ...cur, ...n,
          tracks: (n.tracks.length > cur.tracks.length) ? n.tracks : cur.tracks,
          sections: (n.sections.length > cur.sections.length) ? n.sections : cur.sections,
          featuredItems: (n.featuredItems.length > cur.featuredItems.length) ? n.featuredItems : cur.featuredItems,
          subItems: (n.subItems.length > cur.subItems.length) ? n.subItems : cur.subItems,
        });
      }
    }
  }

  const items = Array.from(byUrl.values());
  const merged = {
    lastUpdated: new Date().toISOString(),
    country: 'us',
    phase: 'merged',
    totalFiles: files.length,
    totalItems: items.length,
    totalTracks: items.reduce((s, i) => s + (i.tracks?.length || 0), 0),
    items,
    sourceFiles: files.map(f => path.basename(f)),
  };
  saveJson(outFile, merged);
  console.log(`[merge] ${files.length} files -> ${outFile} (${items.length} items)`);
  return merged;
}

async function runOrchestrator() {
  ensureDataDirs();

  console.log('========================================');
  console.log('Apple Music Orchestrator');
  console.log(`Per worker: ${MAX_BROWSERS} browsers, ${PARALLEL_PER_BROWSER} per browser (${TOTAL_TASK_LOOPS} loops)`);
  console.log(`Flow: depth1 -> 3x depth2 -> 9x depth3 -> merge -> artist workers -> final merge`);
  console.log('========================================');

  const orchestratorState = {
    stage: 'starting',
    completedWorkers: 0,
    totalWorkers: 1 + DEPTH2_SPLITS + (DEPTH2_SPLITS * DEPTH3_SPLITS_PER_DEPTH2) + ARTIST_WORKERS,
  };
  const stopHeartbeat = startHeartbeat('orchestrator', () => orchestratorState);

  // 1) Depth 1
  orchestratorState.stage = 'depth1';
  const d1wf = writeWorkflow('depth1.json', {
    kind: 'crawl',
    phase: 'depth1',
    workerId: 'depth1-main',
    depth: 1,
    queue: SEED_URLS.map(url => ({ url, depth: 1 })),
    includeMandatoryArtists: true,
    outputName: 'us-depth1.json',
    emitNextDepthLinksFile: workflowPath('depth1-next-links.json'),
  });
  await spawnNode(['--mode', 'worker', '--workflow', d1wf], path.join(LOG_DIR, 'depth1.log'));
  orchestratorState.completedWorkers += 1;

  // 2) Depth 2 split into 3
  orchestratorState.stage = 'depth2';
  const d1next = loadJson(workflowPath('depth1-next-links.json'), { links: [] });
  const d2candidates = dedupeByUrl((d1next.links || []).map(l => ({ url: l.url, depth: 2 })));
  const d2splits = splitArray(d2candidates, DEPTH2_SPLITS);

  const d2wfs = [];
  for (let i = 0; i < DEPTH2_SPLITS; i++) {
    d2wfs.push(writeWorkflow(`depth2-w${i + 1}.json`, {
      kind: 'crawl',
      phase: 'depth2',
      workerId: `depth2-w${i + 1}`,
      depth: 2,
      queue: d2splits[i],
      outputName: `us-depth2-w${i + 1}.json`,
      emitNextDepthLinksFile: workflowPath(`depth2-w${i + 1}-next-links.json`),
    }));
  }
  await Promise.all(d2wfs.map((wf, i) =>
    spawnNode(['--mode', 'worker', '--workflow', wf], path.join(LOG_DIR, `depth2-w${i + 1}.log`))
      .then(() => { orchestratorState.completedWorkers += 1; })
  ));

  // 3) Depth 3 split each depth2 into 3 => 9
  orchestratorState.stage = 'depth3';
  const d3wfs = [];
  for (let i = 0; i < DEPTH2_SPLITS; i++) {
    const d2next = loadJson(workflowPath(`depth2-w${i + 1}-next-links.json`), { links: [] });
    const d3candidates = dedupeByUrl((d2next.links || []).map(l => ({ url: l.url, depth: 3 })));
    const d3splits = splitArray(d3candidates, DEPTH3_SPLITS_PER_DEPTH2);

    for (let j = 0; j < DEPTH3_SPLITS_PER_DEPTH2; j++) {
      d3wfs.push(writeWorkflow(`depth3-w${i + 1}-${j + 1}.json`, {
        kind: 'crawl',
        phase: 'depth3',
        workerId: `depth3-w${i + 1}-${j + 1}`,
        depth: 3,
        queue: d3splits[j],
        outputName: `us-depth3-w${i + 1}-${j + 1}.json`,
      }));
    }
  }
  await Promise.all(d3wfs.map(wf => {
    const id = path.basename(wf, '.json');
    return spawnNode(['--mode', 'worker', '--workflow', wf], path.join(LOG_DIR, `${id}.log`))
      .then(() => { orchestratorState.completedWorkers += 1; });
  }));

  // 4) Merge crawl-only
  orchestratorState.stage = 'merge-crawl';
  const crawlMergedFile = path.join(DATA_DIR, 'us-crawl-only.json');
  await runMerge(path.join(OUTPUT_DIR, 'us-depth*.json'), crawlMergedFile);

  // 5) Artist pipeline
  orchestratorState.stage = 'artists';
  const artistSplits = splitArray(TOP_ARTISTS, ARTIST_WORKERS);
  const artistWfs = [];
  for (let i = 0; i < ARTIST_WORKERS; i++) {
    artistWfs.push(writeWorkflow(`artists-w${i + 1}.json`, {
      kind: 'artist-batch',
      phase: 'artists',
      workerId: `artists-w${i + 1}`,
      artists: artistSplits[i],
      outputName: `us-artists-w${i + 1}.json`,
    }));
  }

  await Promise.all(artistWfs.map((wf, i) =>
    spawnNode(['--mode', 'worker', '--workflow', wf], path.join(LOG_DIR, `artists-w${i + 1}.log`))
      .then(() => { orchestratorState.completedWorkers += 1; })
  ));

  // 6) Final merge
  orchestratorState.stage = 'merge-final';
  await runMerge(path.join(OUTPUT_DIR, '*.json'), FINAL_OUTPUT);

  orchestratorState.stage = 'done';
  stopHeartbeat();
  console.log(`Done. Final output: ${FINAL_OUTPUT}`);
}

async function main() {
  try {
    if (MODE === 'orchestrator') return runOrchestrator();
    if (MODE === 'worker') return runWorker(WORKFLOW_FILE);
    if (MODE === 'merge') return runMerge(MERGE_GLOB, FINAL_OUTPUT);
    throw new Error(`Unknown --mode ${MODE}`);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
}

main();
