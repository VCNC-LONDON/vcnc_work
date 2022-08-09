def solution(skills, team, k):
    answer = 0

    for i in range(len(skills) - k + 1) :
        player = [x+i+1 for x in range(k)]
        player_skill = skills[i:i+k]

        if set(team).issubset(player) :
            power = sum(player_skill) * 2
        else :
            power = sum(player_skill)

        if power > answer :
            answer = power

    return answer