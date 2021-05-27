library(tmvtnorm)

# sigma <- matrix(c(5, 0.8, 0.8, 1), 2, 2)
# Fx <- ptmvt(lowerx=c(-1,-1), upperx=c(0.5,0), mean=c(0,0), 
#            sigma=sigma, df=3, lower=c(-1,-1), upper=c(1,1))
#print(Fx)

sigma <- rbind(c(1, 1, 0, 1, 1, 1), c(1, 36, -1, -1, -3, -5),
               c(0, -1, 4, 2, 2, 0), c(1, -1, 2, 49, -5, -2),
               c(1, -3, 2, -5, 16, -2), c(1, -5, 0, -2, -2, 9))

mean <- c(0.004, 0.002, 0.002, 0.004, 0.003, 0.001)
lower=c(-0.1,-0.1,-0.1,-0.1,-0.1,-0.1)
upper=c(0.1,0.1,0.1,0.1,0.1,0.1)


result <- rtmvt(10000, mean = mean, sigma = sigma, df = 4, 
      lower=lower, upper=upper,
      algorithm=c("gibbs"))
 write.csv(result, file="result_1M.csv")

