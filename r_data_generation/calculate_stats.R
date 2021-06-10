wallet <- c(0.2, 0.2, 0.2, 0.15, 0.15, 0.1)

returnRates <- read.csv(file = 'firstFrame.csv')
# returnRates <- subset(returnRates, select = c(V1, V2, V3, V4, V5, V6))
print(returnRates)

# wybór wiersza
print(returnRates[1,])

# wybór kolumny
print(returnRates[,1])

result.mean <- c()
result.median <- c()
result.quantile <- c()
result.avrageOf10PercLowest <- c()
result.mb1 <- c()
result.mb2 <- c()

# średnia
for(i in 1:ncol(returnRates)) {
  # średnia
  result.mean[i] <- mean(returnRates[,i])
  # mediana
  result.median[i] <- median(returnRates[,i])
  # kwantyl rzędu 0,1
  result.quantile[i] <- quantile(returnRates[,i], 0.1)
  # średnia z 10% najmniejszych stóp zwrotu
  sortedCol <- sort(returnRates[,i], decreasing=FALSE)
  tenPercentOfLowest <- sortedCol[0:(length(sortedCol)/10)]
  result.avrageOf10PercLowest[i] <- mean(tenPercentOfLowest)
  #miara bezpieczeństwa oparta na odchyleniu przeciętnym
  helpSum <- 0
  for(retRate in returnRates[,i]) {
    helpSum <- helpSum + abs(result.mean[i] - retRate)
  }

  result.mb1[i] <- result.mean[i] - (helpSum / (2 * length(returnRates[,i])))
  # miarabezpieczeństwaopartanaśredniejróżnicyGiniego
  helpSumOuter <- 0
  helpSumInner <- 0
  for(retRateOuter in returnRates[,i]) {
    helpSumInner <- 0
    for(retRateInner in returnRates[,i]) {
      helpSumInner <- helpSumInner + abs(retRateOuter - retRateInner)
    }
    helpSumOuter <- helpSumOuter + helpSumInner
  }
  print(i)
  print("average")
  print(result.mean[i])
  print("len")
  print(length(returnRates[,1]))
  print("inner")
  print(helpSumInner)
  print("outer")
  print(helpSumOuter)
  result.mb2[i] <- result.mean[i] - (helpSumOuter / (2 * length(returnRates[,i]) * length(returnRates[,i])))
}

fullResult <- data.frame(result.mean, result.median, result.quantile, result.avrageOf10PercLowest, result.mb1, result.mb2)
print(t(fullResult))
