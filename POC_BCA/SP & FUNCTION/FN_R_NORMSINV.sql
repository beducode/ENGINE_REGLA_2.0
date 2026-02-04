CREATE OR REPLACE function       FN_R_NORMSINV(p in number) return number deterministic is
  $if DBMS_DB_VERSION.VERSION > 11 $then
  	pragma udf;
  $end


    type doubleArray is varray(6) of number;

    a doubleArray := doubleArray(
    -3.969683028665376e+01,  2.209460984245205e+02,
    -2.759285104469687e+02,  1.383577518672690e+02,
    -3.066479806614716e+01,  2.506628277459239e+00
    );

    b doubleArray := doubleArray(
    -5.447609879822406e+01,  1.615858368580409e+02,
    -1.556989798598866e+02,  6.680131188771972e+01,
    -1.328068155288572e+01
    );

    c doubleArray := doubleArray(
    -7.784894002430293e-03, -3.223964580411365e-01,
    -2.400758277161838e+00, -2.549732539343734e+00,
     4.374664141464968e+00,  2.938163982698783e+00
    );

    d doubleArray :=doubleArray(
    7.784695709041462e-03,  3.224671290700398e-01,
    2.445134137142996e+00,  3.754408661907416e+00
    );

    q number;
    t number;
    u number;

    c_sqrt_2pi constant number := 2.50662827463100050241576528481104525301;
  begin

    if (p = 0.0) then
      u:=-10000;
      return(u);
    end if;

    if (p = 1.0) then
      u:=10000;
      return(u);
    end if;

    q := least(p,1-p);

    if (q > 0.02425) then
      /* Rational approximation for central region. */
      u := q-0.5;
      t := u*u;
      u := u*(((((a(1)*t+a(2))*t+a(3))*t+a(4))*t+a(5))*t+a(6))
      /(((((b(1)*t+b(2))*t+b(3))*t+b(4))*t+b(5))*t+1);
    else
      /* Rational approximation for both tail regions. */
      t := sqrt(-2*ln(q));
      u := (((((c(1)*t+c(2))*t+c(3))*t+c(4))*t+c(5))*t+c(6))
      /((((d(1)*t+d(2))*t+d(3))*t+d(4))*t+1);
    end if;
     /* The relative error of the approximation has absolute value less
      than 1.15e-9.  One iteration of Halley's rational method (third
      order) gives full machine precision... */
    t:= FN_R_NORMSDIST(u);
    t:=t-q;    /* error */
    t := t*c_sqrt_2pi*exp(u*u/2);   /* f(u)/df(u) */
    u := u-t/(1+u*t/2);     /* Halley's method */

    if(p>0.5) then
      u:=-u;
    end if;
    return(u);
  end;