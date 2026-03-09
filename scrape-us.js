const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const glob = require('glob');

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
const MAX_QUEUE_SIZE = 30000;
const MAX_PARALLEL = 15;
const MAX_BROWSERS = 15;
const DELAY_BETWEEN_PAGES = 200;
const MAX_SUBPAGES_PER_ARTIST = 3;
const MAX_TRACKS_PER_ITEM = 500;
const PAGE_TIMEOUT = 30000;

const PROGRESS_FILE = path.join(__dirname, 'data', 'progress.json');
const DATA_DIR = path.join(__dirname, 'data');

const args = process.argv.slice(2);
const startArg = args.findIndex(a => a === '--start');
const endArg = args.findIndex(a => a === '--end');
const jobArg = args.findIndex(a => a === '--job');

const JOB_START = startArg >= 0 ? parseInt(args[startArg + 1]) : null;
const JOB_END = endArg >= 0 ? parseInt(args[endArg + 1]) : null;
const JOB_INDEX = jobArg >= 0 ? parseInt(args[jobArg + 1]) : null;

// New chunking arguments
const depthArg = args.findIndex(a => a === '--depth');
const chunkArg = args.findIndex(a => a === '--chunk');
const totalChunksArg = args.findIndex(a => a === '--total-chunks');
const queueFileArg = args.findIndex(a => a === '--queue-file');
const mergeArg = args.findIndex(a => a === '--merge');

const CURRENT_DEPTH = depthArg >= 0 ? parseInt(args[depthArg + 1]) : null;
const CHUNK_ID = chunkArg >= 0 ? parseInt(args[chunkArg + 1]) : 1;
const TOTAL_CHUNKS = totalChunksArg >= 0 ? parseInt(args[totalChunksArg + 1]) : 1;
const QUEUE_FILE = queueFileArg >= 0 ? args[queueFileArg + 1] : null;
const DO_MERGE = mergeArg >= 0;

const isParallelMode = JOB_START !== null && JOB_END !== null;
const isChunkedMode = CURRENT_DEPTH !== null;

function generateId(url) {
  const hash = url.split('').reduce((a, b) => {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
  return Math.abs(hash).toString(36);
}

function ensureDataDir() {
  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  return dataDir;
}

function saveProgress(phase, data) {
  try {
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
      timestamp: new Date().toISOString(),
      phase: phase,
      ...data
    }, null, 2));
  } catch (e) {
    console.error('Error saving progress:', e.message);
  }
}

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      console.log(`Resuming from saved progress: ${data.phase} phase`);
      return data;
    }
  } catch (e) {
    console.log('No progress file found, starting fresh');
  }
  return null;
}

function clearProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      fs.unlinkSync(PROGRESS_FILE);
      console.log('Progress file cleared');
    }
  } catch (e) {
    console.error('Error clearing progress:', e.message);
  }
}

function saveQueueChunk(queue, depth, chunkId, totalChunks) {
  try {
    const filename = `depth${depth}-chunk${chunkId}-of${totalChunks}.json`;
    const filepath = path.join(DATA_DIR, filename);
    
    const chunkData = {
      depth: depth,
      chunkId: chunkId,
      totalChunks: totalChunks,
      queue: queue,
      savedAt: new Date().toISOString()
    };
    
    fs.writeFileSync(filepath, JSON.stringify(chunkData, null, 2));
    console.log(`Saved ${queue.length} items to ${filename}`);
    return filepath;
  } catch (e) {
    console.error('Error saving queue chunk:', e.message);
    return null;
  }
}

function loadQueueChunk(chunkId, totalChunks, depth) {
  try {
    const filename = `depth${depth}-chunk${chunkId}-of${totalChunks}.json`;
    const filepath = path.join(DATA_DIR, filename);
    
    if (fs.existsSync(filepath)) {
      const chunkData = JSON.parse(fs.readFileSync(filepath, 'utf8'));
      console.log(`Loaded ${chunkData.queue.length} items from ${filename}`);
      return chunkData.queue;
    }
  } catch (e) {
    console.error('Error loading queue chunk:', e.message);
  }
  return [];
}

function mergeResults(outputFiles, finalOutput) {
  try {
    const allItems = [];
    const seenUrls = new Set();
    
    for (const file of outputFiles) {
      if (fs.existsSync(file)) {
        const data = JSON.parse(fs.readFileSync(file, 'utf8'));
        for (const item of data.items || []) {
          if (!seenUrls.has(item.url)) {
            seenUrls.add(item.url);
            allItems.push(item);
          }
        }
      }
    }
    
    const mergedData = {
      lastUpdated: new Date().toISOString(),
      country: 'us',
      phase: 'merged',
      totalItems: allItems.length,
      totalTracks: allItems.reduce((sum, item) => sum + (item.tracks?.length || 0), 0),
      items: allItems
    };
    
    fs.writeFileSync(path.join(DATA_DIR, finalOutput), JSON.stringify(mergedData, null, 2));
    console.log(`Merged ${outputFiles.length} files with ${allItems.length} total items into ${finalOutput}`);
    return mergedData;
  } catch (e) {
    console.error('Error merging results:', e.message);
    return null;
  }
}

function saveOutputFile(items, phase, checkpoint) {
  try {
    const uniqueItems = [];
    const seenUrls = new Set();
    
    for (const item of items) {
      if (!seenUrls.has(item.url)) {
        seenUrls.add(item.url);
        uniqueItems.push({
          id: generateId(item.url),
          name: item.name,
          type: item.type,
          country: 'us',
          url: item.url,
          searchTerms: item.name.toLowerCase().replace(/[^a-z0-9\s]/g, ' '),
          scrapedAt: new Date().toISOString(),
          creator: item.creator || '',
          metadata: item.metadata || '',
          description: item.description || '',
          tracks: item.tracks || [],
          sections: item.sections || [],
          featuredItems: item.featuredItems || []
        });
      }
    }
    
    const usData = {
      lastUpdated: new Date().toISOString(),
      country: 'us',
      phase: phase,
      checkpoint: checkpoint,
      totalItems: uniqueItems.length,
      totalTracks: uniqueItems.reduce((sum, item) => sum + (item.tracks?.length || 0), 0),
      items: uniqueItems
    };
    
    const dataDir = ensureDataDir();
    fs.writeFileSync(
      path.join(dataDir, 'us.json'),
      JSON.stringify(usData, null, 2)
    );
    console.log(`    [CHECKPOINT] Saved us.json with ${uniqueItems.length} items`);
    
  } catch (e) {
    console.error('Error saving output file:', e.message);
  }
}

async function scrollUntilExhausted(page, direction = 'vertical') {
  await page.evaluate(async (dir) => {
    const scrollDelay = 800;
    const maxScrolls = 50;
    
    let lastScrollPosition = dir === 'vertical' ? window.scrollY : window.scrollX;
    let samePositionCount = 0;
    
    for (let i = 0; i < maxScrolls; i++) {
      if (dir === 'vertical') {
        window.scrollBy(0, 800);
      } else {
        const containers = document.querySelectorAll('[style*="overflow-x"], .shelf-grid, [class*="carousel"]');
        for (const container of containers) {
          container.scrollBy({ left: 400, behavior: 'smooth' });
        }
        window.scrollBy({ left: 400, top: 0, behavior: 'smooth' });
      }
      
      await new Promise(r => setTimeout(r, scrollDelay));
      
      const currentScrollPosition = dir === 'vertical' ? window.scrollY : window.scrollX;
      
      if (currentScrollPosition === lastScrollPosition) {
        samePositionCount++;
        if (samePositionCount >= 3) break;
      } else {
        samePositionCount = 0;
      }
      
      lastScrollPosition = currentScrollPosition;
    }
    
    if (dir === 'vertical') {
      window.scrollTo(0, 0);
    }
  }, direction);
}

function detectType(url) {
  const lowerUrl = url.toLowerCase();
  
  if (lowerUrl.includes('/artist/')) return 'artist';
  if (lowerUrl.includes('/song/')) return 'song';
  if (lowerUrl.includes('/album/')) return 'album';
  if (lowerUrl.includes('/single/') || lowerUrl.includes('/ep/')) return 'single';
  if (lowerUrl.includes('/playlist/')) return 'playlist';
  if (lowerUrl.includes('/chart/')) return 'chart';
  if (lowerUrl.includes('/radio') || lowerUrl.includes('/station/')) return 'radio';
  
  return 'other';
}

async function extractLinksSectionsAndTracks(page) {
  return await page.evaluate(() => {
    const results = { links: [], sections: [], tracks: [], featuredItems: [] };
    
    function detectType(url) {
      const lowerUrl = url.toLowerCase();
      if (lowerUrl.includes('/artist/')) return 'artist';
      if (lowerUrl.includes('/song/')) return 'song';
      if (lowerUrl.includes('/album/')) return 'album';
      if (lowerUrl.includes('/single/') || lowerUrl.includes('/ep/')) return 'single';
      if (lowerUrl.includes('/playlist/')) return 'playlist';
      if (lowerUrl.includes('/chart/')) return 'chart';
      if (lowerUrl.includes('/radio') || lowerUrl.includes('/station/')) return 'radio';
      if (lowerUrl.includes('/room/')) return 'room';
      return 'other';
    }
    
    // Find all sections with data-testid="section-container" and aria-label="Featured"
    const featuredSections = document.querySelectorAll('[data-testid="section-container"][aria-label="Featured"]');
    
    for (const section of featuredSections) {
      // Find all items within this featured section
      const itemContainers = section.querySelectorAll('[class*="lockup"], a[href*="/playlist/"], a[href*="/album/"], a[href*="/chart/"], a[href*="/room/"]');
      
      for (const container of itemContainers) {
        // Get the link if it's an anchor, otherwise look for anchor inside
        const linkEl = container.tagName === 'A' ? container : container.querySelector('a[href]');
        const url = linkEl ? linkEl.href : '';
        if (!url || !url.includes('music.apple.com')) continue;
        
        const cleanUrl = url.split('?')[0].split('#')[0];
        
        // Extract title from headings__title
        const titleEl = container.querySelector('[class*="headings__title"]') || container.querySelector('[class*="title"]');
        const title = titleEl ? titleEl.textContent?.trim() : '';
        
        // Extract subtitle from headings__subtitles
        const subtitleEl = container.querySelector('[class*="headings__subtitles"]');
        const subtitle = subtitleEl ? subtitleEl.textContent?.trim() : '';
        
        // Check if subtitle is a link (clickable)
        const subtitleLink = subtitleEl ? subtitleEl.closest('a') : null;
        const subtitleUrl = subtitleLink ? subtitleLink.href : '';
        
        // Extract tertiary titles
        const tertiaryEl = container.querySelector('[class*="headings__tertiary-titles"]');
        const tertiaryTitles = tertiaryEl ? tertiaryEl.textContent?.trim() : '';
        
        // Extract metadata-bottom
        const metadataEl = container.querySelector('[class*="headings__metadata-bottom"]');
        const metadata = metadataEl ? metadataEl.textContent?.trim() : '';
        
        // Extract description
        const descEl = container.querySelector('[class*="description"]');
        const description = descEl ? descEl.textContent?.trim() : '';
        
        if (title && cleanUrl) {
          const item = {
            name: title,
            url: cleanUrl,
            type: detectType(cleanUrl),
            creator: subtitle,
            tertiaryTitles: tertiaryTitles,
            metadata: metadata,
            description: description
          };
          
          results.featuredItems.push(item);
          
          // Follow ALL subtitle links (both Apple Music and artist)
          if (subtitleUrl && subtitleUrl.includes('music.apple.com')) {
            const cleanSubUrl = subtitleUrl.split('?')[0];
            if (!results.links.includes(cleanSubUrl)) {
              results.links.push(cleanSubUrl);
            }
          }
        }
      }
    }
    
    // Also get all links from page for crawling
    const anchors = document.querySelectorAll('a[href]');
    for (const a of anchors) {
      const href = a.href;
      if (href && href.includes('music.apple.com/us/')) {
        const cleanUrl = href.split('?')[0].split('#')[0];
        if (!results.links.includes(cleanUrl)) {
          results.links.push(cleanUrl);
        }
      }
    }
    
    // Extract page-level metadata (title, subtitle, description for the current page)
    const pageTitleEl = document.querySelector('[class*="headings__title"]');
    results.pageTitle = pageTitleEl ? pageTitleEl.textContent?.trim() : '';
    
    const pageSubtitleEl = document.querySelector('[class*="headings__subtitles"]');
    results.pageSubtitle = pageSubtitleEl ? pageSubtitleEl.textContent?.trim() : '';
    
    const pageMetaEl = document.querySelector('[class*="headings__metadata-bottom"]');
    results.pageMetadata = pageMetaEl ? pageMetaEl.textContent?.trim() : '';
    
    const pageDescEl = document.querySelector('[class*="description"]');
    results.pageDescription = pageDescEl ? pageDescEl.textContent?.trim() : '';
    
    // Extract tracks from the page
    const trackSelectors = ['div.songs-list-row', 'li.songs-list-item', '[data-testid="track-row"]', 'div[class*="track"]'];
    let trackElements = [];
    
    for (const selector of trackSelectors) {
      const found = document.querySelectorAll(selector);
      if (found.length > 0) {
        trackElements = Array.from(found);
        break;
      }
    }
    
    if (trackElements.length === 0) {
      const allLinks = document.querySelectorAll('a[href]');
      allLinks.forEach(link => {
        if (link.href && link.href.includes('/song/')) {
          trackElements.push(link);
        }
      });
    }
    
    const seenTracks = new Set();
    trackElements.forEach((el, index) => {
      let trackUrl = '';
      let trackName = '';
      let artistName = '';
      let albumName = '';
      let duration = '';
      
      if (el.href && el.href.includes('/song/')) {
        trackUrl = el.href.split('?')[0];
      } else {
        const songLink = el.querySelector('a[href*="/song/"]');
        if (songLink) trackUrl = songLink.href.split('?')[0];
      }
      
      if (!trackUrl || seenTracks.has(trackUrl)) return;
      seenTracks.add(trackUrl);
      
      const nameEl = el.querySelector('[class*="title"], [class*="name"]');
      trackName = nameEl ? nameEl.textContent?.trim() : (el.textContent?.trim() || '');
      trackName = trackName.substring(0, 200);
      
      const artistEl = el.querySelector('[class*="artist"], .by-line');
      if (artistEl) artistName = artistEl.textContent?.trim() || '';
      
      const albumEl = el.querySelector('[class*="album"]');
      if (albumEl) albumName = albumEl.textContent?.trim() || '';
      
      const durationEl = el.querySelector('[class*="duration"], .time');
      if (durationEl) duration = durationEl.textContent?.trim() || '';
      
      if (trackName && trackUrl) {
        results.tracks.push({
          name: trackName,
          url: trackUrl,
          artist: artistName,
          album: albumName,
          duration: duration,
          position: index + 1
        });
      }
    });
    
    return results;
  });
}

async function crawlPage(page, url) {
  try {
    await page.goto(url, { waitUntil: 'networkidle', timeout: PAGE_TIMEOUT });
    await scrollUntilExhausted(page, 'vertical');
    await scrollUntilExhausted(page, 'horizontal');
    const extracted = await extractLinksSectionsAndTracks(page);
    return extracted;
  } catch (error) {
    console.error(`Error crawling ${url}: ${error.message}`);
    return { links: [], sections: [], tracks: [], featuredItems: [], pageTitle: '', pageSubtitle: '', pageMetadata: '', pageDescription: '' };
  }
}

async function processCrawler(browsers, queue, visited, items, currentDepth) {
  let pageCount = 0;
  const browserPool = [...browsers];
  let browserIndex = 0;
  
  function getNextBrowser() {
    const browser = browserPool[browserIndex % browserPool.length];
    browserIndex++;
    return browser;
  }
  
  async function processPage(url, depth) {
    const urlType = detectType(url);
    
    if (urlType === 'song' || urlType === 'radio') {
      return { processed: false, isAppleMusic: false, links: [], tracks: [], featuredItems: [], pageSubtitle: '', pageTitle: '' };
    }
    
    const browser = getNextBrowser();
    const page = await browser.newPage();
    
    try {
      const { links: pageLinks, sections, tracks, featuredItems, pageTitle, pageSubtitle, pageMetadata, pageDescription } = await crawlPage(page, url);
      
      const pageType = detectType(url);
      const isSeedPage = depth === 1;
      const isAppleMusicPage = pageSubtitle && pageSubtitle.toLowerCase().includes('apple music');
      
      if ((isSeedPage || isAppleMusicPage) && (tracks.length > 0 || featuredItems.length > 0) && pageType !== 'radio') {
        const pageName = pageTitle || url.split('/').pop() || 'Unknown';
        const existingItem = items.find(i => i.url === url);
        
        if (existingItem) {
          existingItem.tracks = tracks.slice(0, MAX_TRACKS_PER_ITEM);
          existingItem.sections = sections;
          existingItem.featuredItems = featuredItems;
          existingItem.creator = pageSubtitle;
          existingItem.metadata = pageMetadata;
          existingItem.description = pageDescription;
        } else {
          items.push({
            name: pageName,
            url: url,
            type: pageType,
            creator: pageSubtitle || 'Apple Music',
            metadata: pageMetadata,
            description: pageDescription,
            tracks: tracks.slice(0, MAX_TRACKS_PER_ITEM),
            sections: sections,
            featuredItems: featuredItems
          });
        }
      }
      
      let newLinks = [];
      const isAlbum = pageType === 'album';
      
      if ((isSeedPage || isAppleMusicPage) && queue.length <= MAX_QUEUE_SIZE) {
        const shouldSkipAlbum = isAlbum && pageLinks.length === 0;
        
        if (!shouldSkipAlbum) {
          for (const link of pageLinks) {
            if (!visited.has(link)) {
              const linkType = detectType(link);
              if (linkType !== 'song' && linkType !== 'radio') {
                newLinks.push({ url: link, depth: depth + 1 });
              }
            }
          }
        }
      }
      
      return { 
        processed: true, 
        isAppleMusic: isSeedPage || isAppleMusicPage,
        links: newLinks, 
        tracks: tracks.length, 
        featuredItems: featuredItems.length, 
        pageSubtitle: pageSubtitle || 'N/A',
        pageTitle: pageTitle
      };
      
    } catch (error) {
      console.error(`    Error: ${error.message}`);
      return { processed: false, isAppleMusic: false, links: [], tracks: 0, featuredItems: 0, pageSubtitle: '', pageTitle: '' };
    } finally {
      await page.close();
    }
  }
  
  let depth1Complete = false;
  let depth2Complete = false;
  
  while (queue.length > 0) {
    if (queue.length > MAX_QUEUE_SIZE) {
      console.log(`\n=== QUEUE LIMIT EXCEEDED (${MAX_QUEUE_SIZE}) - NO MORE LINKS WILL BE ADDED ===\n`);
    }
    
    const batch = [];
    const batchDepth = queue[0].depth;
    
    while (queue.length > 0 && batch.length < MAX_PARALLEL) {
      const item = queue.shift();
      if (!visited.has(item.url) && item.depth <= MAX_DEPTH) {
        batch.push(item);
      }
    }
    
    if (batch.length === 0) break;
    
    const results = await Promise.all(batch.map(item => {
      visited.add(item.url);
      return processPage(item.url, item.depth);
    }));
    
    pageCount += results.filter(r => r.processed).length;
    
    let newLinksToAdd = [];
    for (const result of results) {
      if (result.links && result.links.length > 0) {
        newLinksToAdd.push(...result.links);
      }
    }
    
    // Track which depth we're at
    const currentBatchDepth = batch[0].depth;
    
    // Save queue chunk when depth changes
    if (currentBatchDepth === 1 && !depth1Complete && newLinksToAdd.length > 0) {
      // Save all depth 2 links as one chunk
      const depth2Links = newLinksToAdd.filter(l => l.depth === 2);
      if (depth2Links.length > 0 && TOTAL_CHUNKS > 1) {
        // Split into chunks
        const chunkSize = Math.ceil(depth2Links.length / TOTAL_CHUNKS);
        for (let c = 0; c < TOTAL_CHUNKS; c++) {
          const start = c * chunkSize;
          const end = start + chunkSize;
          const chunk = depth2Links.slice(start, end);
          if (chunk.length > 0) {
            saveQueueChunk(chunk, 2, c + 1, TOTAL_CHUNKS);
          }
        }
      }
      depth1Complete = true;
    }
    
    if (currentBatchDepth === 2 && !depth2Complete && newLinksToAdd.length > 0) {
      const depth3Links = newLinksToAdd.filter(l => l.depth === 3);
      if (depth3Links.length > 0 && TOTAL_CHUNKS > 1) {
        const chunkSize = Math.ceil(depth3Links.length / TOTAL_CHUNKS);
        for (let c = 0; c < TOTAL_CHUNKS; c++) {
          const start = c * chunkSize;
          const end = start + chunkSize;
          const chunk = depth3Links.slice(start, end);
          if (chunk.length > 0) {
            saveQueueChunk(chunk, 3, c + 1, TOTAL_CHUNKS);
          }
        }
      }
      depth2Complete = true;
    }
    
    // Add remaining links to queue (for single workflow mode)
    if (TOTAL_CHUNKS === 1) {
      for (const link of newLinksToAdd) {
        if (!visited.has(link.url) && !queue.find(q => q.url === link.url)) {
          queue.push(link);
        }
      }
    }
    
    const sample = results[0];
    const batchTracks = results.reduce((sum, r) => sum + (r.tracks || 0), 0);
    const allTotalTracks = items.reduce((sum, item) => sum + (item.tracks?.length || 0), 0);
    const subtitles = [...new Set(results.map(r => r.pageSubtitle).filter(s => s))].join(', ');
    
    console.log(`Pages Crawled: ${pageCount} | Depth: ${batchDepth}/${MAX_DEPTH} | Processed Pages: ${results.length} | Total Processed Pages: ${pageCount} | Queue: ${queue.length}/${MAX_QUEUE_SIZE} | Items: ${items.length} | Tracks from Batch: ${batchTracks} | All Total Tracks: ${allTotalTracks} | Subtitle of Batch: ${subtitles}`);
    
    if (pageCount % 10 === 0) {
      saveProgress('crawl', { queue: queue.slice(0, 100), visited: Array.from(visited), pageCount: pageCount });
      saveOutputFile(items, 'crawl', pageCount);
    }
    
    await new Promise(r => setTimeout(r, DELAY_BETWEEN_PAGES));
  }
  
  // Save final queue for chunking if in multi-chunk mode
  if (TOTAL_CHUNKS > 1 && queue.length > 0) {
    const nextDepth = queue[0]?.depth || (currentDepth + 1);
    saveQueueChunk(queue, nextDepth, CHUNK_ID, TOTAL_CHUNKS);
  }
  
  return pageCount;
}

async function findArtistUrl(browser, artistName) {
  const searchUrl = `https://music.apple.com/us/search?term=${encodeURIComponent(artistName)}`;
  const page = await browser.newPage();
  
  try {
    await page.goto(searchUrl, { waitUntil: 'networkidle', timeout: PAGE_TIMEOUT });
    await scrollUntilExhausted(page, 'vertical');
    
    const artistLink = await page.evaluate((name) => {
      const links = document.querySelectorAll('a[href]');
      for (const link of links) {
        const href = link.href;
        const text = link.textContent?.trim() || '';
        if (href && href.includes('/artist/') && text.toLowerCase() === name.toLowerCase()) {
          return href.split('?')[0];
        }
      }
      const firstArtistLink = Array.from(links).find(l => l.href && l.href.includes('/artist/') && l.href.split('/artist/')[1]);
      return firstArtistLink ? firstArtistLink.href.split('?')[0] : null;
    }, artistName);
    
    return artistLink;
  } finally {
    await page.close();
  }
}

async function processArtistPage(browser, artistName, artistUrl, visited) {
  const page = await browser.newPage();
  const artistData = { name: artistName, url: artistUrl, type: 'artist', sections: [], subItems: [] };
  
  try {
    await page.goto(artistUrl, { waitUntil: 'networkidle', timeout: PAGE_TIMEOUT });
    await scrollUntilExhausted(page, 'vertical');
    await scrollUntilExhausted(page, 'horizontal');
    
    const extracted = await extractLinksSectionsAndTracks(page);
    artistData.sections = extracted.sections;
    
    const subPageUrls = new Set();
    for (const section of extracted.sections) {
      for (const item of section.items) {
        const itemType = detectType(item.url);
        if (['album', 'single', 'playlist', 'artist-essential', 'artist-setlist', 'artist-playlist'].includes(itemType)) {
          artistData.subItems.push({ name: item.name, url: item.url, type: itemType, tracks: [] });
          if (!visited.has(item.url)) subPageUrls.add(item.url);
        }
      }
    }
    
    console.log(`    Found ${artistData.sections.length} sections with ${artistData.subItems.length} sub-items`);
    
    const subPageList = Array.from(subPageUrls).slice(0, MAX_SUBPAGES_PER_ARTIST);
    for (let i = 0; i < subPageList.length; i++) {
      const subUrl = subPageList[i];
      console.log(`    [${i + 1}/${subPageList.length}] Scraping: ${subUrl.substring(0, 40)}...`);
      
      const subPage = await browser.newPage();
      try {
        await subPage.goto(subUrl, { waitUntil: 'networkidle', timeout: PAGE_TIMEOUT });
        await scrollUntilExhausted(subPage, 'vertical');
        
        const subData = await extractLinksSectionsAndTracks(subPage);
        
        const subItem = artistData.subItems.find(s => s.url === subUrl);
        if (subItem) subItem.tracks = subData.tracks.slice(0, MAX_TRACKS_PER_ITEM);
        
        visited.add(subUrl);
      } catch (e) {
        console.error(`    Error: ${e.message}`);
      } finally {
        await subPage.close();
        await new Promise(r => setTimeout(r, 500));
      }
    }
    
    return artistData;
  } finally {
    await page.close();
  }
}

async function runCrawl() {
  console.log('========================================');
  console.log('Apple Music US Crawler (Queue Limit: ' + MAX_QUEUE_SIZE + ')');
  console.log(`Max Browsers: ${MAX_BROWSERS} | Max Parallel: ${MAX_PARALLEL}`);
  if (isChunkedMode) {
    console.log(`CHUNKED MODE: Depth ${CURRENT_DEPTH}, Chunk ${CHUNK_ID} of ${TOTAL_CHUNKS}`);
  }
  console.log('========================================\n');
  
  ensureDataDir();
  let progress = loadProgress();
  
  let queue, visited, items, artistsProcessed;
  
  // Handle merge mode
  if (DO_MERGE) {
    console.log('=== MERGING RESULTS ===');
    const glob = require('glob');
    const files = glob.sync(path.join(DATA_DIR, 'us-*.json'));
    const merged = mergeResults(files, 'us.json');
    console.log(`Merge complete! Total items: ${merged?.totalItems || 0}`);
    return;
  }
  
  // Load queue from chunk file if specified
  if (QUEUE_FILE) {
    const queuePath = path.join(DATA_DIR, QUEUE_FILE);
    if (fs.existsSync(queuePath)) {
      const queueData = JSON.parse(fs.readFileSync(queuePath, 'utf8'));
      queue = queueData.queue || [];
      console.log(`Loaded ${queue.length} URLs from ${QUEUE_FILE}`);
    } else {
      console.error(`Queue file not found: ${QUEUE_FILE}`);
      return;
    }
  } else if (progress && progress.phase === 'crawl') {
    queue = progress.queue || [];
    visited = new Set(progress.visited || []);
    artistsProcessed = 0;
    
    const existingDataPath = path.join(__dirname, 'data', 'us.json');
    if (fs.existsSync(existingDataPath)) {
      try {
        const existingData = JSON.parse(fs.readFileSync(existingDataPath, 'utf8'));
        items = existingData.items || [];
        console.log(`Loaded ${items.length} existing items from us.json`);
      } catch (e) {
        items = [];
      }
    } else {
      items = [];
    }
  } else if (isChunkedMode && CURRENT_DEPTH > 1) {
    // Load specific chunk
    queue = loadQueueChunk(CHUNK_ID, TOTAL_CHUNKS, CURRENT_DEPTH);
    visited = new Set();
    items = [];
    artistsProcessed = 0;
  } else {
    queue = SEED_URLS.map(url => ({ url, depth: 1 }));
    visited = new Set();
    items = [];
    artistsProcessed = 0;
  }
  
  // Launch multiple browsers
  console.log(`Launching ${MAX_BROWSERS} browsers...`);
  const browsers = [];
  for (let i = 0; i < MAX_BROWSERS; i++) {
    browsers.push(await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] }));
  }
  
  try {
    console.log(`=== PHASE 1: CRAWLER ===\n`);
    console.log(`Starting crawl with ${queue.length} URLs... Max depth: ${MAX_DEPTH}, Max queue: ${MAX_QUEUE_SIZE}\n`);
    
    await processCrawler(browsers, queue, visited, items, 1);
    
    console.log('\n=== PHASE 2: MANDATORY ARTISTS ===\n');
    
    const mainBrowser = browsers[0];
    for (let i = artistsProcessed || 0; i < MANDATORY_ARTISTS.length; i++) {
      const artist = MANDATORY_ARTISTS[i];
      console.log(`[Mandatory ${i + 1}/${MANDATORY_ARTISTS.length}] Processing: ${artist.name}`);
      
      const artistData = await processArtistPage(mainBrowser, artist.name, artist.url, visited);
      items.push(artistData);
      
      artistsProcessed = i + 1;
      saveProgress('mandatory', { artistsProcessed: artistsProcessed });
      saveOutputFile(items, 'mandatory', i + 1);
      
      const delayPage = await mainBrowser.newPage();
      await delayPage.waitForTimeout(DELAY_BETWEEN_PAGES);
      await delayPage.close();
    }
    
    console.log('\nCrawl complete! Total items: ' + items.length);
    clearProgress();
  } finally {
    for (const browser of browsers) {
      await browser.close();
    }
  }
}

async function runParallelArtists() {
  console.log('========================================');
  console.log(`Apple Music US - Parallel Artist Processing`);
  console.log(`Job #${JOB_INDEX}: Artists ${JOB_START} to ${JOB_END}`);
  console.log('========================================\n');
  
  ensureDataDir();
  let progress = loadProgress();
  
  let visited, items, artistStartIndex;
  
  if (progress && progress.phase === 'artist' && progress.jobIndex === JOB_INDEX) {
    visited = new Set(progress.visited || []);
    artistStartIndex = progress.artistIndex || 0;
    
    const existingDataPath = path.join(__dirname, 'data', 'us.json');
    if (fs.existsSync(existingDataPath)) {
      try {
        const existingData = JSON.parse(fs.readFileSync(existingDataPath, 'utf8'));
        items = existingData.items || [];
        console.log(`Loaded ${items.length} existing items from us.json`);
      } catch (e) {
        items = [];
      }
    } else {
      items = [];
    }
  } else {
    visited = new Set();
    items = [];
    artistStartIndex = 0;
  }
  
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  
  const artistsToProcess = TOP_ARTISTS.slice(JOB_START, JOB_END);
  console.log(`Processing ${artistsToProcess.length} artists...\n`);
  
  try {
    for (let i = artistStartIndex; i < artistsToProcess.length; i++) {
      const artistName = artistsToProcess[i];
      const globalIndex = JOB_START + i;
      
      console.log(`[Artist ${globalIndex + 1}/${TOP_ARTISTS.length}] Processing: ${artistName}`);
      
      const artistUrl = await findArtistUrl(browser, artistName);
      
      if (artistUrl) {
        const artistData = await processArtistPage(browser, artistName, artistUrl, visited);
        items.push(artistData);
        console.log(`    Collected: ${artistData.sections.length} sections, ${artistData.subItems.length} items`);
      } else {
        console.log(`    No artist page found`);
      }
      
      if (i % 5 === 0) {
        saveProgress('artist', { jobIndex: JOB_INDEX, artistIndex: i + 1, visited: Array.from(visited) });
        saveOutputFile(items, 'artist', i + 1);
        console.log(`    Progress saved at artist ${i + 1}`);
      }
      
      const delayPage = await browser.newPage();
      await delayPage.waitForTimeout(DELAY_BETWEEN_PAGES);
      await delayPage.close();
    }
    
    saveOutputFile(items, 'artist', artistsToProcess.length);
    console.log(`\nArtist processing complete! Items collected: ${items.length}`);
  } finally {
    await browser.close();
  }
}

async function main() {
  if (isParallelMode) {
    await runParallelArtists();
  } else {
    await runCrawl();
  }
}

main().catch(console.error);
