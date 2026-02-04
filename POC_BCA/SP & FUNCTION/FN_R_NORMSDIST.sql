CREATE OR REPLACE function       FN_R_NORMSDIST(u in number) return number deterministic is
  $if DBMS_DB_VERSION.VERSION > 11 $then
  	pragma udf;
  $end


    type doubleArray is varray(9) of number;

    a doubleArray := doubleArray (
    1.161110663653770e-002,3.951404679838207e-001,2.846603853776254e+001,
    1.887426188426510e+002,3.209377589138469e+003
    );

    b doubleArray := doubleArray (
    1.767766952966369e-001,8.344316438579620e+000,1.725514762600375e+002,
    1.813893686502485e+003,8.044716608901563e+003
    );

    c doubleArray := doubleArray (
    2.15311535474403846e-8,5.64188496988670089e-1,8.88314979438837594e00,
    6.61191906371416295e01,2.98635138197400131e02,8.81952221241769090e02,
    1.71204761263407058e03,2.05107837782607147e03,1.23033935479799725E03
    );

    d doubleArray := doubleArray (
    1.00000000000000000e00,1.57449261107098347e01,1.17693950891312499e02,
    5.37181101862009858e02,1.62138957456669019e03,3.29079923573345963e03,
    4.36261909014324716e03,3.43936767414372164e03,1.23033935480374942e03
    );

    p doubleArray := doubleArray (
    1.63153871373020978e-2,3.05326634961232344e-1,3.60344899949804439e-1,
    1.25781726111229246e-1,1.60837851487422766e-2,6.58749161529837803e-4
    );

    q doubleArray := doubleArray (
    1.00000000000000000e00,2.56852019228982242e00,1.87295284992346047e00,
    5.27905102951428412e-1,6.05183413124413191e-2,2.33520497626869185e-3
    );

    z number;
    y number;
    outValue number;

    c_sqrt2 constant number := 1.41421356237309504880168872420969807857;
    c_sqrt2_0$46875 constant number := 0.6629126073623883041257915894732959743297;
    c_sqrt2_4 constant number := 5.65685424949238019520675489683879231428;
    c_sqrt_pi constant number := 1.7724538509055160272981674833411451828;
  begin


    y := abs(u);
    if (y <= c_sqrt2_0$46875) then
    /* evaluate erf() for |u| <= sqrt(2)*0.46875 */
      z := y*y;
      y := u*((((a(1)*z+a(2))*z+a(3))*z+a(4))*z+a(5))
         /((((b(1)*z+b(2))*z+b(3))*z+b(4))*z+b(5));

      outValue:=0.5+y;
      return(outValue);
    end if;

    z := exp(-y*y/2)/2;
    if (y <= c_sqrt2_4) then
    /* evaluate erfc() for sqrt(2)*0.46875 <= |u| <= sqrt(2)*4.0 */
      y := y/c_sqrt2;
      y :=
      ((((((((c(1)*y+c(2))*y+c(3))*y+c(4))*y+c(5))*y+c(6))*y+c(7))*y+c(8))*y+c(9))
      /((((((((d(1)*y+d(2))*y+d(3))*y+d(4))*y+d(5))*y+d(6))*y+d(7))*y+d(8))*y+d(9));
      y := z*y;
    else
    /* evaluate erfc() for |u| > sqrt(2)*4.0 */
      z := z*c_sqrt2/y;
      y := 2/(y*y);
      y := y*(((((p(1)*y+p(2))*y+p(3))*y+p(4))*y+p(5))*y+p(6))
      /(((((q(1)*y+q(2))*y+q(3))*y+q(4))*y+q(5))*y+q(6));
      y := z*(1/c_sqrt_pi-y);
    end if;

    if(u<0) then outValue:=y;
    else outValue:=1-y; end if;

    return(outValue);

  end;