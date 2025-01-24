package main

import "testing"

func Test_RoundTime(t *testing.T) {
	// Function test for roundTime

	type args struct {
		timestamp    int64
		roundSeconds int64
	}

	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "Should return rounded time 10 seconds",
			args: args{
				timestamp:    1737644288,
				roundSeconds: 10,
			},
			want: 1737644280,
		},
		{
			name: "Should return rounded time 60 seconds",
			args: args{
				timestamp:    1737644288,
				roundSeconds: 60,
			},
			want: 1737644280,
		},
		{
			name: "Should return rounded time 300 seconds",
			args: args{
				timestamp:    1737644288,
				roundSeconds: 300,
			},
			want: 1737644100,
		},
		{
			name: "Should return rounded time 1 hour",
			args: args{
				timestamp:    1737644288,
				roundSeconds: 3600,
			},
			want: 1737640800,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := roundTime(tt.args.timestamp, tt.args.roundSeconds); got != tt.want {
				t.Errorf("RoundTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
