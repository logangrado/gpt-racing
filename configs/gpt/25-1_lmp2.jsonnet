{
  points: {
    drop_races: 1,
    points: {
      default: [25, 21, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
      large: [32, 27, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
      major: [38, 32, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
    },
    fastest_lap: {
      points: 1,
      must_be_on_lead_lap: true,
    },
    cleanest_driver: {
      points: 1,
      must_be_on_lead_lap: true,
    },
  },
  elo: {
    previous_seasons: [
      import '24_gt3.jsonnet',
    ],
    min_races: 4,
    time_window: '365 days',
  },
  races: [
    {
      subsession_id: 73468848,
      race_name: 'Algarve',
    },
    {
      subsession_id: 74170897,
      race_name: 'Motegi',
    },
    {
      subsession_id: 74512470,
      race_name: 'Watkins Glen',
    },
    {
      subsession_id: 74849000,
      race_name: 'Road America',
    },
    {
      subsession_id: 75196373,
      race_name: 'Mt Panorama',
    },
    { subsession_id: 75512497, race_name: 'Hockenheimring' },
    { subsession_id: 75823595, race_name: 'Spa' },
  ],
}
