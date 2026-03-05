{
  points: {
    drop_races: 2,
    points: {
      default: [50, 41, 36, 33, 30, 27, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 3, 2, 1],
      major: [75, 64, 59, 55, 51, 48, 45, 42, 39, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 5, 4, 3, 2, 1],
    },
  },
  elo: {
    previous_seasons: [
      import '24_gt3.jsonnet',
      import '25-1_lmp2.jsonnet',
      import '25-2_gt4.jsonnet',
    ],
    min_races: 4,
    time_window: '365 days',
    weight: 0.5,
  },
  races: [
    {
      subsession_id: 82684096,
      race_name: 'Interlagos',
      penalties: [
        // Minkler
        { user_id: 48076, time: 5 },
        // Mann
        { user_id: 603983, time: 5 },
      ],
    },
    {
      subsession_id: 83060140,
      race_name: 'Roat Atlanta',
      penalties: [
        { user_id: 335343, time: 5 },
        { user_id: 464635, time: 5 },
        { user_id: 622340, time: 5 },
      ],
    },
    {
      subsession_id: 83429093,
      race_name: 'Imola',
      penalties: [
        {
          user_id: 436882,  //Nevil
          time: 10,
        },
      ],
    },
    {
      subsession_id: 83800241,
      race_name: 'Sebfing',
      penalties: [
        {
          user_id: 346566,
          time: 5,
        },
      ],
    },
  ],
}
