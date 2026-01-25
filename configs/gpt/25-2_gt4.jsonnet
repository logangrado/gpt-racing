{
  points: {
    drop_races: 2,
    points: {
      default: [25, 21, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
      large: [32, 27, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
      major: [38, 32, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
    },
  },
  elo: {
    previous_seasons: [
      import '24_gt3.jsonnet',
      import '25-1_lmp2.jsonnet',
    ],
    min_races: 4,
    time_window: '365 days',
    weight: 0.5,
  },
  races: [
    {
      subsession_id: 77351364,
      race_name: 'Summit',
      penalties: [
        // Facciolo
        { user_id: 335343, time: 5 },
        // Green
        { user_id: 48282, time: 5 },
        // Neilsen
        { user_id: 407523, time: 5 },
        // Yost
        { user_id: 282802, time: 5 },
      ],
    },
    {
      subsession_id: 77670136,
      race_name: 'Donington',
      penalties: [
        // # Holland
        { user_id: 982936, time: 5 },
        // # Cyphers
        { user_id: 346566, time: 5 },
      ],
    },
    {
      subsession_id: 77978233,
      race_name: 'Interlagos',
      points_type: 'major',
    },
    {
      subsession_id: 78318633,
      race_name: 'CTMP',
      penalties: [
        // # Neilsen
        { user_id: 407523, time: 5 },
      ],
    },
    {
      subsession_id: 78656185,
      race_name: 'Mt. Panorama',
    },
    {
      subsession_id: 78994686,
      race_name: 'Aragon',
    },
    {
      subsession_id: 79331651,
      race_name: 'Nürburgring',
      points_type: 'major',
    },
    {
      subsession_id: 79621636,
      race_name: 'Road Atlanta',
    },
  ],
}
